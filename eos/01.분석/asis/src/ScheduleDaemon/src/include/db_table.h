/**
 *******************************************************************************
 * @brief  db-table header
 * @remark
 * @file   db_table.h
 * @date   2012.12.01
 * @author Ysic
 *******************************************************************************
 */
/**! */
EXEC SQL BEGIN DECLARE SECTION;

/**
 * @brief SCHEDULE ���̺� ����ü
 */
struct TB_SCHEDULE {
    int    schedule_id                    ; ///< ������ ID
    char   type_cd                [  10+1]; ///< ���������ڵ�
    int    scenario_id                    ; ///< �ó����� ID
    char   user_id                [  13+1]; ///< USER ID
    char   prioritize             [  20+1]; ///< �켱����
    char   process_cd             [  20+1]; ///< ó�� �ڵ�
    char   process_msg            [1500+1]; ///< ó�� �޼���
    char   scenario_path          [ 256+1]; ///< �ó����� ��� ���� ���
    char   reg_dt                 [  14+1]; ///< �����
    char   modify_dt              [  14+1]; ///< ������
};

/**
 * @brief SECTOR ���̺���ü
 */
struct TB_SECTOR {
    char   areaname               [  30+1]; ///< Area �̸�
    char   scenarioname           [  30+1]; ///< Scenario �̸�
    char   sitename               [  30+1]; ///< Site �̸�
    int    sectorid                       ; ///< Sector ID
    int    pnoffsetnum                    ; ///< PN OFFSET ��ȣ
    int    sectorseq                      ; ///< ���� ������ȣ
    int    height                         ; ///< ����
    int    feederloss                     ; ///< ���̺�ս�
    int    noisefloor                     ; ///< �� ����
    int    radius                         ; ///< ���͹ݰ�
    int    correctionvalue                ; ///<
    int    fademargin                     ; ///< ���̵帶��
    int    startx                         ; ///< ȭ����ǥ����
    int    starty                         ; ///<
    int    endx                           ; ///< ȭ����ǥ��
    int    endy                           ; ///<
    char   pathlossmodelname      [  30+1]; ///< ���ĸ��̸�
    int    sectorstatus                   ; ///< ����Ȱ��ȭ
};

/**
 * @brief SITE ���̺���ü
 */
struct TB_SITE {
    char   areaname               [  30+1]; ///< Area �̸�
    char   scenarioname           [  30+1]; ///< Scenario �̸�
    char   sitename               [  30+1]; ///< Site �̸�
    int    xposition                      ; ///< ���� ��ǥX
    int    yposition                      ; ///< ���� ��ǥ Y
    int    height                         ; ///< ����
    char   siteid                 [  30+1]; ///< Site ID
    int    type                           ; ///< ������ Type
    char   address                [ 150+1]; ///< �ּ�
    char   maker                  [  30+1]; ///< ������
    int    mscnum                         ; ///< MSC ��ȣ
    int    btsnum                         ; ///< BTS��ȣ
    int    bscnum                         ; ///< BSC��ȣ
    char   imagefilepath           [150+1]; ///< ���ϰ��
    int    systemid                       ; ///< System ID
    int    networkid                      ; ///< Network ID
    int    usabletch                      ; ///< TCH ��ȣ
    int    status                         ; ///< Ȱ��ȭ����
    char   donorsitename          [  30+1]; ///< �߰�� ��� Donor Site
    int    donorsectorid                  ; ///< �߰�� ��� Donor Sector
    int    donorpnoffset                  ; ///< �߰�� ��� Donor PN Offset
    int    repeaterattenuation            ; ///< �߰�� ����
    int    repeaterpwrratio               ; ///< �߰�� �Ŀ� ����
    int    initnetworkidx                 ; ///< Initial Network Design Index
};

/**
 * @brief DBUSER table

struct DBS_DBUSER {
    char   userid               [ 10+1];
    char   password             [ 10+1];
    char   name                 [ 10+1];
    char   homedir              [ 10+1];
    int    usermode                  ;
    char   createdate           [ 21+1];
    char   lastlogindate        [ 21+1];
    char   authority            [ 10+1];
};
 */

EXEC SQL END DECLARE SECTION;
