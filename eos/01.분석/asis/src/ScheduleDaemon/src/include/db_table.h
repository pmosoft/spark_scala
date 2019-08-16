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
 * @brief SCHEDULE 테이블 구조체
 */
struct TB_SCHEDULE {
    int    schedule_id                    ; ///< 스케쥴 ID
    char   type_cd                [  10+1]; ///< 업무구분코드
    int    scenario_id                    ; ///< 시나리오 ID
    char   user_id                [  13+1]; ///< USER ID
    char   prioritize             [  20+1]; ///< 우선순위
    char   process_cd             [  20+1]; ///< 처리 코드
    char   process_msg            [1500+1]; ///< 처리 메세지
    char   scenario_path          [ 256+1]; ///< 시나리오 결과 파일 경로
    char   reg_dt                 [  14+1]; ///< 등록일
    char   modify_dt              [  14+1]; ///< 수정일
};

/**
 * @brief SECTOR 테이블구조체
 */
struct TB_SECTOR {
    char   areaname               [  30+1]; ///< Area 이름
    char   scenarioname           [  30+1]; ///< Scenario 이름
    char   sitename               [  30+1]; ///< Site 이름
    int    sectorid                       ; ///< Sector ID
    int    pnoffsetnum                    ; ///< PN OFFSET 번호
    int    sectorseq                      ; ///< 섹터 고유번호
    int    height                         ; ///< 높이
    int    feederloss                     ; ///< 케이블손실
    int    noisefloor                     ; ///< 열 잡음
    int    radius                         ; ///< 섹터반경
    int    correctionvalue                ; ///<
    int    fademargin                     ; ///< 페이드마진
    int    startx                         ; ///< 화면좌표시작
    int    starty                         ; ///<
    int    endx                           ; ///< 화면좌표끝
    int    endy                           ; ///<
    char   pathlossmodelname      [  30+1]; ///< 전파모델이름
    int    sectorstatus                   ; ///< 섹터활성화
};

/**
 * @brief SITE 테이블구조체
 */
struct TB_SITE {
    char   areaname               [  30+1]; ///< Area 이름
    char   scenarioname           [  30+1]; ///< Scenario 이름
    char   sitename               [  30+1]; ///< Site 이름
    int    xposition                      ; ///< 지도 좌표X
    int    yposition                      ; ///< 지도 좌표 Y
    int    height                         ; ///< 높이
    char   siteid                 [  30+1]; ///< Site ID
    int    type                           ; ///< 기지국 Type
    char   address                [ 150+1]; ///< 주소
    char   maker                  [  30+1]; ///< 제조사
    int    mscnum                         ; ///< MSC 번호
    int    btsnum                         ; ///< BTS번호
    int    bscnum                         ; ///< BSC번호
    char   imagefilepath           [150+1]; ///< 파일경로
    int    systemid                       ; ///< System ID
    int    networkid                      ; ///< Network ID
    int    usabletch                      ; ///< TCH 번호
    int    status                         ; ///< 활성화여부
    char   donorsitename          [  30+1]; ///< 중계기 경우 Donor Site
    int    donorsectorid                  ; ///< 중계기 경우 Donor Sector
    int    donorpnoffset                  ; ///< 중계기 경우 Donor PN Offset
    int    repeaterattenuation            ; ///< 중계기 감쇄
    int    repeaterpwrratio               ; ///< 중계기 파워 비율
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
