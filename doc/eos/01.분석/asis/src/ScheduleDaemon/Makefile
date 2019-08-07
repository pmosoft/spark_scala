################################################################################
# ScheduleDaemon Makefile
#
# author Ysic
# since 2012.12 SureComm
#
# notice.
# 0. Makefile이 있는 곳에서 make 할것
# 1. Makefile 내 tab 문자가 없어지지 않도록 주의
# 2. /opt/oracle/product/11.2.0/precomp/admin/pcscfg.cfg 파일내의 include 경로 확인
################################################################################
#### Make option
## CPU갯수 + CPU갯수의 20%
CPU_NUM=$(shell cat /proc/cpuinfo | grep processor | wc -l)
MAKEOPTS="-j$(shell printf "%.f" `echo ${CPU_NUM}+${CPU_NUM}*0.2 | bc`)"

#### Makefile directory
MAKE_DIR=/home/icpap/TEOS_RELEASE/server/trunk/ScheduleDaemon

#### Configuration file
CONFIGURATION_FILE=ScheduleDaemon.conf

#### blank
BLANK=

#### Compiler
CC=gcc
CFLAGS=-g

#### Source & object & etc directory
SRC_DIR=$(MAKE_DIR)/src
OBJ_DIR=$(MAKE_DIR)/obj
## Pro*C pre-compile target directory
PRECOMP_DIR=$(MAKE_DIR)/precomp
EXE_DIR=$(MAKE_DIR)/bin
CONF_DIR=$(MAKE_DIR)/conf
SCRIPT_DIR=$(MAKE_DIR)/script

#### Install directory
INSTALL_DIR=$(HOME)/CellPLAN
INSTALL_DIR_EXE=$(INSTALL_DIR)/bin
INSTALL_DIR_CONF=$(INSTALL_DIR)/conf
INSTALL_DIR_SCRIPT=$(INSTALL_DIR)/script
INSTALL_DIR_LOG=$(INSTALL_DIR)/log

#### ICP-Library directory
LIB_ICP=$(HOME)/ganji/libicp

#### Pro*C
PROC=proc
PROC_FLAGS=lines=yes dbms=v8 unsafe_null=yes mode=oracle
PROC_INCLUDE=include=$(ORACLE_HOME)/precomp/public include=/usr/include include=$(LIB_ICP)/include include=$(SRC_DIR)/include

#### Include file
INCLUDE=-I/usr/include -I$(SRC_DIR)/include -I$(ORACLE_HOME)/precomp/public -I$(LIB_ICP)/include

#### Library
LIBS=-L$(ORACLE_HOME)/lib -L$(LIB_ICP)/lib
LDFLAGS=-lclntsh -licp

#### Source directories
SRC_DIRS=$(SRC_DIR)/lib \
         $(SRC_DIR)/service \
         $(SRC_DIR)

## Pro*C sources
PROC_SRC=$(foreach dir,$(SRC_DIRS),$(wildcard $(dir)/*.pc))
PRECOMP_SRCS=$(subst $(SRC_DIR),$(PRECOMP_DIR),$(PROC_SRC))
PRECOMP_SRCS:=$(PRECOMP_SRCS:.pc=.c)
PROC_OBJS=$(subst $(SRC_DIR),$(OBJ_DIR),$(PROC_SRC))
PROC_OBJS:=$(PROC_OBJS:.pc=.o)

## C sources
C_SRCS=$(foreach dir,$(SRC_DIRS),$(wildcard $(dir)/*.c))
C_OBJS=$(subst $(SRC_DIR),$(OBJ_DIR),$(C_SRCS))
C_OBJS:=$(C_OBJS:.c=.o)

#### Execution-file source directory
EXESRC_DIR=$(SRC_DIR)
## EXE OBJS
EXE_SRCS=$(foreach TEMP,$(EXESRC_DIR),$(wildcard $(TEMP)/*.c))
EXE_SRCS+=$(foreach TEMP,$(EXESRC_DIR),$(wildcard $(TEMP)/*.pc))
EXE_OBJS=$(subst $(SRC_DIR),$(OBJ_DIR),$(EXE_SRCS))
EXE_OBJS:=$(EXE_OBJS:.c=.o)
EXE_OBJS:=$(EXE_OBJS:.pc=.o)
EXE_FILES=$(subst $(OBJ_DIR),$(EXE_DIR),$(EXE_OBJS))

#### Link objects(exclude execution object)
LINK_OBJS=$(foreach TEMP, $(C_OBJS) $(PROC_OBJS), $(if $(findstring $(TEMP), $(EXE_OBJS)), $(BLANK), $(TEMP)))

#### target all
all:
	@$(MAKE) $(MAKEOPTS) -f $(MAKE_DIR)/Makefile $(PRECOMP_SRCS)
	@$(MAKE) $(MAKEOPTS) -f $(MAKE_DIR)/Makefile $(PROC_OBJS)
	@$(MAKE) $(MAKEOPTS) -f $(MAKE_DIR)/Makefile $(C_OBJS)
	@$(MAKE) -f $(MAKE_DIR)/Makefile $(EXE_FILES)
	@$(MAKE) -f $(MAKE_DIR)/Makefile install
	
	@echo ">> compile done"

#### target Pro*C pre-compile
$(PRECOMP_DIR)/%.c: $(SRC_DIR)/%.pc
	@echo ">> Pro*C Pre-Compiling $< $@"

	@## Create precomp directory
	@[ -d $(PRECOMP_DIR)/$(*D) ] || mkdir -p $(PRECOMP_DIR)/$(*D)

	@echo "$(PROC) $(PROC_INCLUDE) iname=$< oname=$@"

	@$(PROC) $(PROC_FLAGS) $(PROC_INCLUDE) iname=$< oname=$@

#### target Pro*C compile
$(OBJ_DIR)/%.o: $(PRECOMP_DIR)/%.c
	@echo ">> Pro*C Compiling $<"

	@## Create object directory
	@[ -d $(OBJ_DIR)/$(*D) ] || mkdir -p $(OBJ_DIR)/$(*D)

	@$(CC) $(CFLAGS) $(INCLUDE) -c -o $@ $<

#### target C compile
$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c
	@echo ">> Compiling $<"

	@## Create object directory
	@[ -d $(OBJ_DIR)/$(*D) ] || mkdir -p $(OBJ_DIR)/$(*D)

	@$(CC) $(CFLAGS) $(INCLUDE) -c -o $@ $<

#### target Execution-file make
$(EXE_FILES):
	@## Create execution-file directory
	@[ -d $(EXE_DIR) ] || mkdir -p $(EXE_DIR)
	
	@$(CC) -o $(basename $@) $(subst $(EXE_DIR),$(OBJ_DIR),$@) $(LINK_OBJS) $(LIBS) $(LDFLAGS) $(LIB_ICP)/lib/libicp.a
	@echo ">> created $(basename $@)"

#### target Install
install:
	@## Create install directory
	@[ -d $(INSTALL_DIR) ] || mkdir -p $(INSTALL_DIR)

	@## Create bin directory
	@[ -d $(INSTALL_DIR_EXE) ] || mkdir -p $(INSTALL_DIR_EXE)

	@## Create conf directory
	@[ -d $(INSTALL_DIR_CONF) ] || mkdir -p $(INSTALL_DIR_CONF)

	@## Create script directory
	@[ -d $(INSTALL_DIR_SCRIPT) ] || mkdir -p $(INSTALL_DIR_SCRIPT)

	@## Create log directory
	@[ -d $(INSTALL_DIR_LOG) ] || mkdir -p $(INSTALL_DIR_LOG)

	@## Copy files
	@cp $(SCRIPT_DIR)/* $(INSTALL_DIR_SCRIPT)
	@[ -e $(INSTALL_DIR_CONF)/$(CONFIGURATION_FILE) ] || cp $(CONF_DIR)/$(CONFIGURATION_FILE) $(INSTALL_DIR_CONF)
	@cp -i $(EXE_DIR)/* $(INSTALL_DIR_EXE)

	@echo ">> Install success"

#### target delete object & execution-file & etc
clean:
	@rm -rf $(PRECOMP_DIR)
	@rm -rf $(OBJ_DIR)
	@rm -rf $(EXE_DIR)
	@find $(MAKE_DIR) -name '*.lis' -exec rm {} \;
