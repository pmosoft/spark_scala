/**
 *******************************************************************************
 * @brief  JOB SCHEDULE에 등록된 예측 SCHEDULE을 한건씩 읽어 분석서버에 요청한다.
 * @remark
 * @code
 * Usage PredictScheduleDaemon {start|stop}
 * @endcode
 * @file   PredictScheduleDaemon.c
 * @date   2012.12.01
 * @author Ysic
 *******************************************************************************
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <arpa/inet.h>
#include <ctype.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>
#include "common.h"
#include "packet.h"

#define CONFIG_FILE     "./conf/ScheduleDaemon_SC951.conf"   // 설정파일명

/**
 * @brief daemon on/off flag
 */
static int s_daemonOnOffFlag = TRUE;

/**
 * @brief process name = argv[0]
 */
static char s_processName[100];

/**
 * Function prototype
 */
static void signalHandler(int signum);
static void startProcess(void);
static void stopProcess(void);
static void printUsage(int argc, char* argv[]);
static void doSchedule();

/**
 * @brief main
 */
int main(int argc, char* argv[])
{
    int ret = 0;

    if(argc < 2)
    {
        printUsage(argc, argv);
    }

    if(!(strcmp(argv[1], "start") == 0 || strcmp(argv[1], "stop") == 0))
    {
        printUsage(argc, argv);
        return 0;
    }

    // 프로세스명 SET
    memset(s_processName, 0x00, sizeof(s_processName));
    sprintf(s_processName, "%s start", argv[0]);

    // Load configuration file
    cfg_loadConfig(CONFIG_FILE);

    // 로그기본 정보 SET
    log_setLogInfo(cfg_getString("log_level")
                  ,cfg_getString("log_directory")
                  ,"SC951_"
                  ,NULL
                  ,cfg_getInt("log_buffersize"));

    // DB 기본정보 SET
    db_setDBinfo(cfg_getString("db_user")
                ,cfg_getString("db_pwd")
                ,cfg_getString("db_sid"));

    if(strcmp(argv[1], "start") == 0)
    {
        // Process를 실행 시킴
        startProcess();
    }
    else if(strcmp(argv[1], "stop") == 0)
    {
        // Process를 종료 시킴
        stopProcess();
    }

    return 0;
}

/**
 * @brief signal handler
 */
static void signalHandler(int signum)
{
    log_info(__FFILE__, __FFUNC__, __LINE__, "Stopping Schedule Daemon! (Catch SIGTERM signal)");

    // while()문 break
    s_daemonOnOffFlag = 0;
}

/**
 * @brief daemon start
 */
static void startProcess()
{
    int PID = 0;
    pid_t pid;

    // 현재 실행중인 process가 있는지 검사
    PID = sys_getProcessID(s_processName);
    if(!(PID == 0 || PID == (int)getpid())) {
        printf("이미실행중입니다. [%d]\n", PID);
        exit(0);
    }

    if((pid = fork()) < 0) {
        printf("자식프로세스 fork 실패 데몬을 종료 합니다.");
        exit(0);
    } else if (pid != 0) {
        // 부모프로세스를 종료
        //printf("부모프로세스가 종료 되었습니다.\n");
        exit(0);
    }

    // 새로운 세션을 생성
    setsid();

    /*--------------------------------------------------------------------------
        SIGNAL 처리
        shell> kill -l
         1) SIGHUP       2) SIGINT       3) SIGQUIT      4) SIGILL
         5) SIGTRAP      6) SIGABRT      7) SIGBUS       8) SIGFPE
         9) SIGKILL     10) SIGUSR1     11) SIGSEGV     12) SIGUSR2
        13) SIGPIPE     14) SIGALRM     15) SIGTERM     16) SIGSTKFLT
        17) SIGCHLD     18) SIGCONT     19) SIGSTOP     20) SIGTSTP
        21) SIGTTIN     22) SIGTTOU     23) SIGURG      24) SIGXCPU
        25) SIGXFSZ     26) SIGVTALRM   27) SIGPROF     28) SIGWINCH
        29) SIGIO       30) SIGPWR      31) SIGSYS      34) SIGRTMIN
        35) SIGRTMIN+1  36) SIGRTMIN+2  37) SIGRTMIN+3  38) SIGRTMIN+4
        39) SIGRTMIN+5  40) SIGRTMIN+6  41) SIGRTMIN+7  42) SIGRTMIN+8
        43) SIGRTMIN+9  44) SIGRTMIN+10 45) SIGRTMIN+11 46) SIGRTMIN+12
        47) SIGRTMIN+13 48) SIGRTMIN+14 49) SIGRTMIN+15 50) SIGRTMAX-14
        51) SIGRTMAX-13 52) SIGRTMAX-12 53) SIGRTMAX-11 54) SIGRTMAX-10
        55) SIGRTMAX-9  56) SIGRTMAX-8  57) SIGRTMAX-7  58) SIGRTMAX-6
        59) SIGRTMAX-5  60) SIGRTMAX-4  61) SIGRTMAX-3  62) SIGRTMAX-2
        63) SIGRTMAX-1  64) SIGRTMAX
    --------------------------------------------------------------------------*/
    signal(SIGTERM, signalHandler);
    signal(SIGSEGV, signalHandler);
    signal(SIGINT , signalHandler);
    signal(SIGKILL, signalHandler);
    signal(SIGABRT, signalHandler);

    // 스케쥴처리
    doSchedule();
}

/**
 * @brief daemon stop
 */
static void stopProcess(void)
{
    int PID = 0;

    // 현재 실행중인 process가 있는지 검사
    PID = sys_getProcessID(s_processName);
    if(PID == 0) {
        printf("실행중인 프로세스가 없습니다. !\n");
        return;
    }

    printf("스케쥴데몬종료시그널을 보냈습니다. 프로세스를 확인하세요.[%d] \n", PID);
    kill(PID, SIGTERM); // SIGKILL
}

/**
 * @brief print usage
 */
static void printUsage(int argc, char* argv[])
{
    printf("Usage :\n");
    printf("   %s  <start|stop> \n", argv[0]);
    printf("   start : Start %s daemon system.\n", argv[0]);
    printf("   stop  : Stop %s daemon system.\n", argv[0]);
}

/**
 * @brief SCHEDULE테이블을 읽어 현재 수행할 일에 대한 처리를 한다.
 */
static void doSchedule()
{
    int i, ret = 0;
    struct ST_SERVER            st_server;
    struct ST_ANALYSIS_WEIGHT   st_analysis_weight;
    int waitSeconds = cfg_getInt("daemon_wait_seconds");

    log_info(__FFILE__, __FFUNC__, __LINE__, "\n\n\n\n\nSchedule Daemon Start! %d초 간격으로 처리됨", waitSeconds);

    // DB Connect
    ret = db_connect();
    if(ret != SUCC) {
        log_error(__FFILE__, __FFUNC__, __LINE__, "--------------------------   에러정보출력   --------------------------");
        log_error(__FFILE__, __FFUNC__, __LINE__, "SQL코드    [%d]", ret);
        log_error(__FFILE__, __FFUNC__, __LINE__, "----------------------------------------------------------------------");
        return;
    }

    // 분석서버 정보 가져오기
    getServerInfo(&st_server);

    // 분석기능 가중치 환경정보 가져오기
    getAnalysisWeight(&st_analysis_weight);

    // 서버별 분석처리 진행상태 정보 가져오기
    getServerStatus(&st_server);

    for(i=0; i<st_server.server_cnt; i++)
    {
        log_debug(__FFILE__, __FFUNC__, __LINE__, "[server_id-%s] [server_grade-%d] [current_weight-%d] [idle_weight-%d]",
                st_server.server_id[i],
                st_server.server_grade[i],
                st_server.total_weight[i],
                st_server.server_grade[i] - st_server.total_weight[i]   );
    }

    while(s_daemonOnOffFlag) {

        // connection test
        if(db_testCall() != SQLSUCCESS) {
            // DB Disconnect
            db_disconnect();

            // DB Connect
            ret = db_connect();
            if(ret != SQLSUCCESS) {
                log_error(__FFILE__, __FFUNC__, __LINE__, "--------------------------   에러정보출력   --------------------------");
                log_error(__FFILE__, __FFUNC__, __LINE__, "DB연결 오류 발생 SQL코드  = ORA[%d]", ret);
                log_error(__FFILE__, __FFUNC__, __LINE__, "----------------------------------------------------------------------");

                // 관리자 SMS전송
                sms_sendSystemManager("ScheduleDaemon_SC951 DB연결 오류 발생. ORA[%d]", ret);

                return;
            }
        }

        // schedule 처리 로직 호출
        ret = procSchedule_SC951(&st_server, &st_analysis_weight);

        // 잠시쉬고...
        sleep(waitSeconds);

        // YSIC 테스트
        // break;
    }

    // DB Disconnect
    ret = db_disconnect();
    if(ret != SQLSUCCESS)
    {
        log_error(__FFILE__, __FFUNC__, __LINE__, "--------------------------   에러정보출력   --------------------------");
        log_error(__FFILE__, __FFUNC__, __LINE__, "SQL코드    ORA[%d]", ret);
        log_error(__FFILE__, __FFUNC__, __LINE__, "----------------------------------------------------------------------");
    }

    log_info(__FFILE__, __FFUNC__, __LINE__, "Schedule Daemon Stop!");
}
