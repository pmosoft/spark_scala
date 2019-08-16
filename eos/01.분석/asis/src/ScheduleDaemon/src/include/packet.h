/**
 *******************************************************************************
 * @brief  �������� header
 * @remark
 * @file   packet.h
 * @date   2012.12.01
 * @author Ysic
 *******************************************************************************
 */

/******************************************************************************/
/**
 * @brief ���� �����wkd
 */
struct IO_COMMON {
    char length             [  5]; ///< ��������(����� + ������)
    char tx_id              [ 10]; ///< "CELLPLAN" ����
    char tx_code            [  3]; ///< 1.������� ����
    char tx_type            [  4]; ///< 1.������� ����
    char tx_day             [  8]; ///< YYYYMMDD
    char tx_time            [  6]; ///< HHMMSS
    char tx_seq             [  9]; ///< �Ϸİ�����ȣ
    char rx_day             [  8]; ///< YYYYMMDD
    char rx_time            [  6]; ///< HHMMSS
    char delimiter          [  2]; ///< ������("->")
    char rx_code            [  4]; ///< 0000:����, �׿� ����
    char rx_msg             [ 80]; ///< ������ ���� MESSAGE
    char rx_code_dtl        [  5]; ///< �����߻��� �����׸� index
    char rx_msg_dtl         [ 80]; ///< �����߻��� �󼼿����޼���
    char user_id            [ 13]; ///< �����ID
    char filler             [ 17]; ///< ����
};

/******************************************************************************/
/**
 * @brief ��������(CPU)��û �Է�����
 */
struct IN_100_1000 {
    char server_id          [ 10]; ///< SERVER ����
    char cpu_cnt            [  2]; ///< CPU ����
    char cpu_speed          [  6]; ///< CPU �ӵ�
    char cpu_idle           [ 10]; ///< CPU ������
};

/**
 * @brief �������¿�û �������
 */
struct OUT_100_1000 {
    char server_id          [ 10]; ///< SERVER ����
    char cpu_cnt            [  2]; ///< CPU ����
    char cpu_speed          [  6]; ///< CPU �ӵ�
    char cpu_idle           [ 10]; ///< CPU ������
};


/******************************************************************************/
/**
 * @brief ��������(Memory)��û �Է�����
 */
struct IN_100_2000 {
    char server_id              [ 10]; ///< SERVER ����
    char real_mem_total_size    [ 10]; ///< REAL �޸� �ѿ뷮
    char real_mem_idle          [  6]; ///< REAL �޸� ���� �뷮
    char swap_mem_total_size    [ 10]; ///< SWAP �޸� �ѿ뷮
    char swap_mem_idle          [  6]; ///< SWAP �޸� ���� �뷮
};

/**
 * @brief �������¿�û �������
 */
struct OUT_100_2000 {
    char server_id              [ 10]; ///< SERVER ����
    char real_mem_total_size    [ 10]; ///< REAL �޸� �ѿ뷮
    char real_mem_idle          [  6]; ///< REAL �޸� ���� �뷮
    char swap_mem_total_size    [ 10]; ///< SWAP �޸� �ѿ뷮
    char swap_mem_idle          [  6]; ///< SWAP �޸� ���� �뷮
};


/******************************************************************************/
/**
 * @brief ������м���û �Է�����
 */
struct IN_200_1000 {
    char schedule_id        [ 20]; ///< SCHEDULE_ID
    char scenario_id        [ 20]; ///< SCENARIO_ID
    char type_cd            [ 10]; ///< ���������ڵ�(SC001:�м�, SC002:ƿƮ�����ϱ�,SC022:ƿƮ�м�,SC003:2D PROFILE,SC004:,GEOMETRY QUERY)
};

/**
 * @brief ������м���û �������
 */
struct OUT_200_1000 {
    char schedule_id        [ 20]; ///< SCHEDULE_ID
    char scenario_id        [ 20]; ///< SCENARIO_ID
    char type_cd            [ 10]; ///< ���������ڵ�(SC001:�м�, SC002:ƿƮ�����ϱ�,SC022:ƿƮ�м�,SC003:2D PROFILE,SC004:,GEOMETRY QUERY)
};
