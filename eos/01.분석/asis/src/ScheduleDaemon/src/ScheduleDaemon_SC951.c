/**
 *******************************************************************************
 * @brief  JOB SCHEDULE�� ��ϵ� ���� SCHEDULE�� �ѰǾ� �о� �м������� ��û�Ѵ�.
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

#define CONFIG_FILE     "./conf/ScheduleDaemon_SC951.conf"   // �������ϸ�

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

    // ���μ����� SET
    memset(s_processName, 0x00, sizeof(s_processName));
    sprintf(s_processName, "%s start", argv[0]);

    // Load configuration file
    cfg_loadConfig(CONFIG_FILE);

    // �αױ⺻ ���� SET
    log_setLogInfo(cfg_getString("log_level")
                  ,cfg_getString("log_directory")
                  ,"SC951_"
                  ,NULL
                  ,cfg_getInt("log_buffersize"));

    // DB �⺻���� SET
    db_setDBinfo(cfg_getString("db_user")
                ,cfg_getString("db_pwd")
                ,cfg_getString("db_sid"));

    if(strcmp(argv[1], "start") == 0)
    {
        // Process�� ���� ��Ŵ
        startProcess();
    }
    else if(strcmp(argv[1], "stop") == 0)
    {
        // Process�� ���� ��Ŵ
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

    // while()�� break
    s_daemonOnOffFlag = 0;
}

/**
 * @brief daemon start
 */
static void startProcess()
{
    int PID = 0;
    pid_t pid;

    // ���� �������� process�� �ִ��� �˻�
    PID = sys_getProcessID(s_processName);
    if(!(PID == 0 || PID == (int)getpid())) {
        printf("�̹̽������Դϴ�. [%d]\n", PID);
        exit(0);
    }

    if((pid = fork()) < 0) {
        printf("�ڽ����μ��� fork ���� ������ ���� �մϴ�.");
        exit(0);
    } else if (pid != 0) {
        // �θ����μ����� ����
        //printf("�θ����μ����� ���� �Ǿ����ϴ�.\n");
        exit(0);
    }

    // ���ο� ������ ����
    setsid();

    /*--------------------------------------------------------------------------
        SIGNAL ó��
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

    // ������ó��
    doSchedule();
}

/**
 * @brief daemon stop
 */
static void stopProcess(void)
{
    int PID = 0;

    // ���� �������� process�� �ִ��� �˻�
    PID = sys_getProcessID(s_processName);
    if(PID == 0) {
        printf("�������� ���μ����� �����ϴ�. !\n");
        return;
    }

    printf("�����쵥������ñ׳��� ���½��ϴ�. ���μ����� Ȯ���ϼ���.[%d] \n", PID);
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
 * @brief SCHEDULE���̺��� �о� ���� ������ �Ͽ� ���� ó���� �Ѵ�.
 */
static void doSchedule()
{
    int i, ret = 0;
    struct ST_SERVER            st_server;
    struct ST_ANALYSIS_WEIGHT   st_analysis_weight;
    int waitSeconds = cfg_getInt("daemon_wait_seconds");

    log_info(__FFILE__, __FFUNC__, __LINE__, "\n\n\n\n\nSchedule Daemon Start! %d�� �������� ó����", waitSeconds);

    // DB Connect
    ret = db_connect();
    if(ret != SUCC) {
        log_error(__FFILE__, __FFUNC__, __LINE__, "--------------------------   �����������   --------------------------");
        log_error(__FFILE__, __FFUNC__, __LINE__, "SQL�ڵ�    [%d]", ret);
        log_error(__FFILE__, __FFUNC__, __LINE__, "----------------------------------------------------------------------");
        return;
    }

    // �м����� ���� ��������
    getServerInfo(&st_server);

    // �м���� ����ġ ȯ������ ��������
    getAnalysisWeight(&st_analysis_weight);

    // ������ �м�ó�� ������� ���� ��������
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
                log_error(__FFILE__, __FFUNC__, __LINE__, "--------------------------   �����������   --------------------------");
                log_error(__FFILE__, __FFUNC__, __LINE__, "DB���� ���� �߻� SQL�ڵ�  = ORA[%d]", ret);
                log_error(__FFILE__, __FFUNC__, __LINE__, "----------------------------------------------------------------------");

                // ������ SMS����
                sms_sendSystemManager("ScheduleDaemon_SC951 DB���� ���� �߻�. ORA[%d]", ret);

                return;
            }
        }

        // schedule ó�� ���� ȣ��
        ret = procSchedule_SC951(&st_server, &st_analysis_weight);

        // ��ý���...
        sleep(waitSeconds);

        // YSIC �׽�Ʈ
        // break;
    }

    // DB Disconnect
    ret = db_disconnect();
    if(ret != SQLSUCCESS)
    {
        log_error(__FFILE__, __FFUNC__, __LINE__, "--------------------------   �����������   --------------------------");
        log_error(__FFILE__, __FFUNC__, __LINE__, "SQL�ڵ�    ORA[%d]", ret);
        log_error(__FFILE__, __FFUNC__, __LINE__, "----------------------------------------------------------------------");
    }

    log_info(__FFILE__, __FFUNC__, __LINE__, "Schedule Daemon Stop!");
}
