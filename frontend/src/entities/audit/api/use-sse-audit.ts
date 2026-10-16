import { useEffect, useCallback, useRef } from 'react';
import { useAppDispatch, useAppSelector } from '@/app/store/hooks';
import { env } from '@/shared/config/env';
import { 
  setSseConnected, 
  setAllAuditDone, 
  setCurrentMessage, 
  updateStepStatus,
  setHouseAuditData,
  setCreditData,
  setLoanData,
  setOcrData,
  addErrorMessage
} from '../model/audit.slice';
import { setReviewData } from '@/entities/review/model/review.slice';
import { fetchVerificationResult } from '@/entities/verification/api/verification.api';
import { fetchReviewResult } from '@/entities/review/api/review.api';

/**
 * @feature Audit/SSE
 * 백엔드 SSE(Server-Sent Events) 스트림에 연결하여 실시간 심사진행 상태와 데이터를 Redux에 업데이트하는 훅입니다.
 * (Why) 네이티브 EventSource를 사용하되, 인증 토큰을 쿼리 파라미터(accessToken)로 전달합니다.
 * fetch 기반이나 XHR 폴리필보다 네이티브 API가 Vite 프록시 환경에서 가장 안정적인 스트리밍을 보장합니다.
 */
export const useSseAudit = (consultationId: string | undefined, isTriggered: boolean) => {
  const dispatch = useAppDispatch();
  const { isSseConnected } = useAppSelector((state) => state.audit);
  const accessToken = useAppSelector((state) => state.auth.user?.accessToken);

  // (Why) 값을 ref로 보관하여 useCallback 의존성에서 제거합니다.
  const accessTokenRef = useRef(accessToken);
  const consultationIdRef = useRef(consultationId);
  const eventSourceRef = useRef<EventSource | null>(null);

  useEffect(() => { accessTokenRef.current = accessToken; }, [accessToken]);
  useEffect(() => { consultationIdRef.current = consultationId; }, [consultationId]);

  /**
   * OCR 완료 이벤트 비동기 핸들러
   */
  const handleOcrCompleted = useCallback(async (payload: { message?: string }) => {
    dispatch(updateStepStatus({ step: 'ocr', status: 'LOADING' }));
    try {
      const currentConsultationId = consultationIdRef.current;
      if (!currentConsultationId) return;
      const ocrResult = await fetchVerificationResult(currentConsultationId);
      dispatch(setOcrData(ocrResult));
      dispatch(updateStepStatus({ step: 'ocr', status: 'SUCCESS', message: payload.message || 'OCR 분석 완료' }));
    } catch (error) {
      console.error('[SSE] OCR result fetch failed:', error);
      dispatch(updateStepStatus({ step: 'ocr', status: 'ERROR', message: 'OCR 데이터 수취 실패' }));
      dispatch(addErrorMessage('OCR 데이터를 불러오는 데 실패했습니다.'));
    }
  }, [dispatch]);

  /**
   * 최종 레포트 완료 이벤트 비동기 핸들러
   * (Why) SSE 알림 수신 즉시 실데이터 API를 호출하여 Redux에 저장하고 UI를 갱신합니다.
   */
  const handleReportCompleted = useCallback(async () => {
    try {
      const currentConsultationId = consultationIdRef.current;
      if (!currentConsultationId) return;

      console.log('[SSE] Fetching final report for:', currentConsultationId);
      const reportData = await fetchReviewResult(currentConsultationId);
      
      // 1. 레포트 데이터 전역 상태 저장 (ReviewSlice)
      dispatch(setReviewData(reportData));
      
      // 2. 심사 완료 상태 전이
      dispatch(setAllAuditDone(true));
      dispatch(setCurrentMessage('모든 심사가 완료되었습니다. 결과를 확인하세요.'));
    } catch (error) {
      console.error('[SSE] Report fetch failed:', error);
      dispatch(addErrorMessage('최종 레포트 데이터를 불러오는 데 실패했습니다.'));
    } finally {
      // (Why) 스트림을 명시적으로 정리합니다.
      eventSourceRef.current?.close();
      dispatch(setSseConnected(false));
    }
  }, [dispatch]);

  const connectSse = useCallback(() => {
    if (!consultationId || !isTriggered) return;

    // (Why) 이전 연결이 있으면 먼저 종료합니다.
    eventSourceRef.current?.close();

    const token = accessTokenRef.current;
    
    /**
     * (Why) URLSearchParams는 공백을 +로 인코딩하지만, 
     * 인증 필터는 %20(표준 공백 인코딩)을 기대하는 경우가 많으므로 직접 인코딩합니다.
     */
    const baseUrl = `${window.location.origin}${env.apiBaseUrl}/notification/subscribe`;
    const finalUrl = token 
      ? `${baseUrl}?accessToken=${token}`
      : baseUrl;
    
    console.log('[SSE] Connecting to:', finalUrl.split('accessToken=')[0] + 'accessToken=Bearer ***');
    
    const eventSource = new EventSource(finalUrl, { withCredentials: true });
    eventSourceRef.current = eventSource;

    // ─── 연결 관리 ───
    eventSource.onopen = () => {
      console.log('[SSE] Connection opened (onopen)');
      dispatch(setSseConnected(true));
      dispatch(setCurrentMessage('서버와 실시간 연결됨...'));
    };

    // 0. 초기 연결 확인 이벤트
    eventSource.addEventListener('connect', (event: MessageEvent) => {
      console.log('[SSE] "connect" event received:', event.data);
      dispatch(setSseConnected(true));
      dispatch(setCurrentMessage('심사 프로세스를 시작합니다...'));
    });

    // 보조 메시지 (이름 없는 이벤트)
    eventSource.onmessage = (event) => {
      console.log('[SSE] Standard message received:', event.data);
    };

    // ─── 1. OCR 이벤트 ───
    eventSource.addEventListener('OCR_COMPLETED', (e: MessageEvent) => {
      console.log('[SSE] OCR_COMPLETED received:', e.data);
      const payload = JSON.parse(e.data);
      handleOcrCompleted(payload);
    });
    eventSource.addEventListener('OCR_FAILED', () => {
      dispatch(updateStepStatus({ step: 'ocr', status: 'ERROR', message: 'OCR 분석 실패' }));
      dispatch(addErrorMessage('OCR 처리에 실패했습니다.'));
    });

    // ─── 2. 주택 심사 이벤트 ───
    eventSource.addEventListener('HOUSE_AUDIT_STARTED', (e: MessageEvent) => {
      const payload = JSON.parse(e.data);
      dispatch(updateStepStatus({ step: 'houseAudit', status: 'LOADING', message: `주택 심사 시작: ${payload.data?.propertyAddress || ''}` }));
    });
    eventSource.addEventListener('HOUSE_AUDIT_COMPLETED', (e: MessageEvent) => {
      const payload = JSON.parse(e.data);
      dispatch(setHouseAuditData(payload.data));
      dispatch(updateStepStatus({ step: 'houseAudit', status: 'SUCCESS', message: payload.message || '주택 심사 완료' }));
    });
    eventSource.addEventListener('HOUSE_AUDIT_FAILED', () => {
      dispatch(updateStepStatus({ step: 'houseAudit', status: 'ERROR', message: '주택 심사 실패' }));
      dispatch(addErrorMessage('주택 심사 중 오류가 발생했습니다.'));
    });

    // 주택 심사 세부 단계
    eventSource.addEventListener('HOUSE_AUDIT_PRICE_CHECK_STARTED', () => dispatch(setCurrentMessage('주택 시세 조회 시작...')));
    eventSource.addEventListener('HOUSE_AUDIT_PRICE_CHECK_COMPLETED', () => dispatch(setCurrentMessage('주택 시세 조회 완료')));
    eventSource.addEventListener('HOUSE_AUDIT_PRICE_CHECK_FAILED', () => dispatch(addErrorMessage('주택 시세 조회 중 실패했습니다.')));
    eventSource.addEventListener('HOUSE_AUDIT_ILLEGAL_BUILDING_CHECK_STARTED', () => dispatch(setCurrentMessage('위반 건축물 검사 시작...')));
    eventSource.addEventListener('HOUSE_AUDIT_ILLEGAL_BUILDING_CHECK_COMPLETED', () => dispatch(setCurrentMessage('위반 건축물 검사 완료')));
    eventSource.addEventListener('HOUSE_AUDIT_ILLEGAL_BUILDING_CHECK_FAILED', () => dispatch(addErrorMessage('위반 건축물 검증에 실패했습니다.')));
    eventSource.addEventListener('HOUSE_AUDIT_NEAREST_BRANCH_CHECK_STARTED', () => dispatch(setCurrentMessage('관할 지점 확인 중...')));
    eventSource.addEventListener('HOUSE_AUDIT_NEAREST_BRANCH_CHECK_COMPLETED', () => dispatch(setCurrentMessage('관할 지점 조회 완료')));
    eventSource.addEventListener('HOUSE_AUDIT_NEAREST_BRANCH_CHECK_FAILED', () => dispatch(addErrorMessage('관할 지점 조회에 실패했습니다.')));

    // ─── 3. 마이데이터 이벤트 ───
    eventSource.addEventListener('MY_DATA_AUDIT_STARTED', () => dispatch(setCurrentMessage('마이데이터 스크래핑 시작...')));
    eventSource.addEventListener('MY_DATA_AUDIT_COMPLETED', () => dispatch(setCurrentMessage('마이데이터 심사 전체 완료')));
    eventSource.addEventListener('MY_DATA_AUDIT_FAILED', () => dispatch(addErrorMessage('마이데이터 심사 통합 조회에 실패했습니다.')));
    eventSource.addEventListener('MY_DATA_MEMBER_LOOKUP_STARTED', () => dispatch(setCurrentMessage('마이데이터 회원 여부 조회 중...')));
    eventSource.addEventListener('MY_DATA_MEMBER_LOOKUP_COMPLETED', () => dispatch(setCurrentMessage('마이데이터 회원 확인 완료')));
    eventSource.addEventListener('MY_DATA_MEMBER_LOOKUP_FAILED', () => dispatch(addErrorMessage('마이데이터 회원 조회 중 오류가 발생했습니다.')));

    // 신용등급
    eventSource.addEventListener('MY_DATA_CREDIT_RATING_LOOKUP_STARTED', () => {
      dispatch(updateStepStatus({ step: 'credit', status: 'LOADING', message: '신용등급 조회 중...' }));
    });
    eventSource.addEventListener('MY_DATA_CREDIT_RATING_LOOKUP_COMPLETED', (e: MessageEvent) => {
      const payload = JSON.parse(e.data);
      dispatch(setCreditData(payload.data));
      dispatch(updateStepStatus({ step: 'credit', status: 'SUCCESS', message: payload.message || '신용등급 조회 완료' }));
    });
    eventSource.addEventListener('MY_DATA_CREDIT_RATING_LOOKUP_FAILED', () => {
      dispatch(updateStepStatus({ step: 'credit', status: 'ERROR', message: '신용등급 조회 실패' }));
      dispatch(addErrorMessage('신용등급 조회에 실패했습니다.'));
    });

    // 대출 상품
    eventSource.addEventListener('MY_DATA_LOAN_PRODUCTS_LOOKUP_STARTED', () => {
      dispatch(updateStepStatus({ step: 'loanHistory', status: 'LOADING', message: '기존 대출 내역 조회 중...' }));
    });
    eventSource.addEventListener('MY_DATA_LOAN_PRODUCTS_LOOKUP_COMPLETED', (e: MessageEvent) => {
      const payload = JSON.parse(e.data);
      dispatch(setLoanData(payload.data));
      dispatch(updateStepStatus({ step: 'loanHistory', status: 'SUCCESS', message: payload.message || '대출 내역 조회 완료' }));
    });
    eventSource.addEventListener('MY_DATA_LOAN_PRODUCTS_LOOKUP_FAILED', () => {
      dispatch(updateStepStatus({ step: 'loanHistory', status: 'ERROR', message: '대출 내역 조회 실패' }));
      dispatch(addErrorMessage('대출 내역 조회 중 오류가 발생했습니다.'));
    });

    // ─── 4. 최종 레포트 완료 ───
    eventSource.addEventListener('REPORT_COMPLETED', () => {
      console.log('[SSE] REPORT_COMPLETED received');
      handleReportCompleted();
    });
    // (Why) 백엔드가 재시도를 모두 소진하면 REPORT_FAILED를 보낸다. 받지 않으면 화면이 계속 대기 상태로 남는다.
    eventSource.addEventListener('REPORT_FAILED', () => {
      dispatch(updateStepStatus({ step: 'report', status: 'ERROR', message: '리포트 생성 실패' }));
      dispatch(addErrorMessage('최종 리포트 생성에 실패했습니다. 다시 시도해 주세요.'));
      eventSource.close();
      dispatch(setSseConnected(false));
    });

    // ─── 에러 처리 ───
    eventSource.onerror = (error) => {
      console.error('[SSE] Connection Error:', error);
      if (eventSource.readyState === EventSource.CLOSED) {
        dispatch(setSseConnected(false));
        eventSource.close();
      }
    };

    return () => {
      eventSource.close();
      dispatch(setSseConnected(false));
    };
  }, [consultationId, isTriggered, dispatch, handleOcrCompleted, handleReportCompleted]);

  useEffect(() => {
    const cleanup = connectSse();
    return cleanup;
  }, [connectSse]);

  return { isSseConnected };
};
