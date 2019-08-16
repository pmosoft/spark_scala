/**
 *******************************************************************************
 * @brief  전문관련 header
 * @remark
 * @file   packet.h
 * @date   2012.12.01
 * @author Ysic
 *******************************************************************************
 */

/******************************************************************************/
/**
 * @brief 전문 공통부wkd
 */
struct IO_COMMON {
    char length             [  5]; ///< 전문길이(공통부 + 개별부)
    char tx_id              [ 10]; ///< "CELLPLAN" 고정
    char tx_code            [  3]; ///< 1.업무목록 참조
    char tx_type            [  4]; ///< 1.업무목록 참조
    char tx_day             [  8]; ///< YYYYMMDD
    char tx_time            [  6]; ///< HHMMSS
    char tx_seq             [  9]; ///< 일렬고유번호
    char rx_day             [  8]; ///< YYYYMMDD
    char rx_time            [  6]; ///< HHMMSS
    char delimiter          [  2]; ///< 구분자("->")
    char rx_code            [  4]; ///< 0000:정상, 그외 오류
    char rx_msg             [ 80]; ///< 업무별 응답 MESSAGE
    char rx_code_dtl        [  5]; ///< 에러발생시 에러항목 index
    char rx_msg_dtl         [ 80]; ///< 에러발생시 상세에러메세지
    char user_id            [ 13]; ///< 사용자ID
    char filler             [ 17]; ///< 예비
};

/******************************************************************************/
/**
 * @brief 서버상태(CPU)요청 입력전문
 */
struct IN_100_1000 {
    char server_id          [ 10]; ///< SERVER 구분
    char cpu_cnt            [  2]; ///< CPU 개수
    char cpu_speed          [  6]; ///< CPU 속도
    char cpu_idle           [ 10]; ///< CPU 유휴율
};

/**
 * @brief 서버상태요청 출력전문
 */
struct OUT_100_1000 {
    char server_id          [ 10]; ///< SERVER 구분
    char cpu_cnt            [  2]; ///< CPU 개수
    char cpu_speed          [  6]; ///< CPU 속도
    char cpu_idle           [ 10]; ///< CPU 유휴율
};


/******************************************************************************/
/**
 * @brief 서버상태(Memory)요청 입력전문
 */
struct IN_100_2000 {
    char server_id              [ 10]; ///< SERVER 구분
    char real_mem_total_size    [ 10]; ///< REAL 메모리 총용량
    char real_mem_idle          [  6]; ///< REAL 메모리 남은 용량
    char swap_mem_total_size    [ 10]; ///< SWAP 메모리 총용량
    char swap_mem_idle          [  6]; ///< SWAP 메모리 남은 용량
};

/**
 * @brief 서버상태요청 출력전문
 */
struct OUT_100_2000 {
    char server_id              [ 10]; ///< SERVER 구분
    char real_mem_total_size    [ 10]; ///< REAL 메모리 총용량
    char real_mem_idle          [  6]; ///< REAL 메모리 남은 용량
    char swap_mem_total_size    [ 10]; ///< SWAP 메모리 총용량
    char swap_mem_idle          [  6]; ///< SWAP 메모리 남은 용량
};


/******************************************************************************/
/**
 * @brief 스케쥴분석요청 입력전문
 */
struct IN_200_1000 {
    char schedule_id        [ 20]; ///< SCHEDULE_ID
    char scenario_id        [ 20]; ///< SCENARIO_ID
    char type_cd            [ 10]; ///< 업무구분코드(SC001:분석, SC002:틸트값구하기,SC022:틸트분석,SC003:2D PROFILE,SC004:,GEOMETRY QUERY)
};

/**
 * @brief 스케쥴분석요청 출력전문
 */
struct OUT_200_1000 {
    char schedule_id        [ 20]; ///< SCHEDULE_ID
    char scenario_id        [ 20]; ///< SCENARIO_ID
    char type_cd            [ 10]; ///< 업무구분코드(SC001:분석, SC002:틸트값구하기,SC022:틸트분석,SC003:2D PROFILE,SC004:,GEOMETRY QUERY)
};
