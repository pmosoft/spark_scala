/**
 *******************************************************************************
 * @brief  ���� header
 * @remark
 * @file   common.h
 * @date   2012.12.01
 * @author Ysic
 *******************************************************************************
 */
#include "icp.h"

// 2016-03-30 ganji ���� ���� ���� 10 -> 20
#define	MAX_SERVER	30
/**
 * @brief ���� ����ü. �������� �߻��ϴ� ���������� ������.
 */
struct ST_ERROR
{
    char code        [  4+1]; ///< �����ڵ�
    char message     [ 80+1]; ///< �����޼���
    char filename    [256+1]; ///< �������ϸ�
    char function    [256+1]; ///< �����Լ���
    int  line               ; ///< ��������
    int  sqlcode            ; ///< DB�����ڵ�
    char sqlerrmc    [ 70+1]; ///< DB�����޼��� <= sqlca.sqlerrm.sqlerrmc �� ������ ũ��
};

/**
 * @brief ���� ����� ����ü �� ���� ����ü
 */
struct ST_SVC_PARAM
{
    char *inMessage;            ///< �Է¸޼���
    char *outMessage;           ///< ��¸޼���
    int  inLength;              ///< �Է¸޼�������
    int  outLength;             ///< ��¸޼�������
    struct ST_ERROR error;      ///< ��������
};

/**
 * @brief �м����� ���� ����ü
 */
struct ST_SERVER
{
    int  server_cnt               ;     ///< ���� ����
    char server_id      [ MAX_SERVER][ 10];     ///< ���� ID
    char server_ip      [ MAX_SERVER][ 20];     ///< ���� ���� IP
    int  server_port         [ MAX_SERVER];     ///< ���� ���� Port
    int  cpu_grade           [ MAX_SERVER];     ///< ���� CPU ����
    int  mem_grade           [ MAX_SERVER];     ///< ���� Memory ����
    int  server_grade        [ MAX_SERVER];     ///< ���� ���� ����

    int  schedule_count      [ MAX_SERVER];     ///< ������ ó������ schedule ��
    int  total_weight        [ MAX_SERVER];     ///< ������ ó������ ����ġ �հ�

    int  read_only           [ MAX_SERVER];		///< ���� �м��� ����� �о���� ������ (�м��� ���� ����)
};

/**
 * @brief �м���� ����ġ ���� ����ü
 */
struct ST_ANALYSIS_WEIGHT
{
    int  ru_unit                ;    ///< RU ���� ����
    int  ru_weight              ;    ///< RU ���� ����ġ
    int  bin_unit               ;    ///< BIN ����
    int  bin_weight             ;    ///< BIN ����
};

