#!/bin/sh

source ${HOME}/.bash_profile

cd ${HOME}/CellPlan

if [ "$1" = "start" ]; then
	${PWD}/bin/ScheduleDaemon start
	ps -ef | grep "${PWD}/bin/ScheduleDaemon st" | grep -v grep
elif [ "$1" = "stop" ]; then
	${PWD}/bin/ScheduleDaemon stop
	ps -ef | grep "${PWD}/bin/ScheduleDaemon st" | grep -v grep
else
	echo "Usage : ScheduleDaemon.sh {start|stop}"
fi

cd - >> /dev/null

