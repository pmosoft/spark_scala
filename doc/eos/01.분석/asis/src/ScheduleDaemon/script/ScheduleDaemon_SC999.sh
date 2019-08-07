#!/bin/sh

source ${HOME}/.bash_profile

cd ${HOME}/CellPlan

if [ "$1" = "start" ]; then
	${PWD}/bin/ScheduleDaemon_SC999 start
	ps -ef | grep "${PWD}/bin/ScheduleDaemon_SC999 st" | grep -v grep
elif [ "$1" = "stop" ]; then
	${PWD}/bin/ScheduleDaemon_SC999 stop
	ps -ef | grep "${PWD}/bin/ScheduleDaemon_SC999 st" | grep -v grep
else
	echo "Usage : ScheduleDaemon_SC999.sh {start|stop}"
fi

cd - >> /dev/null

