/**
 *******************************************************************************
 * @brief  공통 header
 * @remark
 * @file   common.h
 * @date   2012.12.01
 * @author Ysic
 *******************************************************************************
 */
#include "icp.h"

// 2016-03-30 ganji 서버 갯수 지정 10 -> 20
#define	MAX_SERVER	30
/**
 * @brief 에러 구조체. 각에러가 발생하는 끝영영에서 설정함.
 */
struct ST_ERROR
{
    char code        [  4+1]; ///< 에러코드
    char message     [ 80+1]; ///< 에러메세지
    char filename    [256+1]; ///< 에러파일명
    char function    [256+1]; ///< 에러함수명
    int  line               ; ///< 에러라인
    int  sqlcode            ; ///< DB에러코드
    char sqlerrmc    [ 70+1]; ///< DB에러메세지 <= sqlca.sqlerrm.sqlerrmc 와 동일한 크기
};

/**
 * @brief 전문 입출력 구조체 및 에러 구조체
 */
struct ST_SVC_PARAM
{
    char *inMessage;            ///< 입력메세지
    char *outMessage;           ///< 출력메세지
    int  inLength;              ///< 입력메세지길이
    int  outLength;             ///< 출력메세지길이
    struct ST_ERROR error;      ///< 에러정보
};

/**
 * @brief 분석서버 정보 구조체
 */
struct ST_SERVER
{
    int  server_cnt               ;     ///< 서버 수량
    char server_id      [ MAX_SERVER][ 10];     ///< 서버 ID
    char server_ip      [ MAX_SERVER][ 20];     ///< 서버 접속 IP
    int  server_port         [ MAX_SERVER];     ///< 서버 접속 Port
    int  cpu_grade           [ MAX_SERVER];     ///< 서버 CPU 수준
    int  mem_grade           [ MAX_SERVER];     ///< 서버 Memory 수준
    int  server_grade        [ MAX_SERVER];     ///< 서버 수준 종합

    int  schedule_count      [ MAX_SERVER];     ///< 서버별 처리중인 schedule 수
    int  total_weight        [ MAX_SERVER];     ///< 서버별 처리중인 가중치 합계

    int  read_only           [ MAX_SERVER];		///< 기존 분석된 결과만 읽어올지 결정함 (분석은 하지 않음)
};

/**
 * @brief 분석기능 가중치 정보 구조체
 */
struct ST_ANALYSIS_WEIGHT
{
    int  ru_unit                ;    ///< RU 수량 단위
    int  ru_weight              ;    ///< RU 수량 가중치
    int  bin_unit               ;    ///< BIN 단위
    int  bin_weight             ;    ///< BIN 수량
};

