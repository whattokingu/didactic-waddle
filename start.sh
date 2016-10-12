#!/bin/bash

if [ "$#" -ne 4 ];
then
	echo "wrong number of arguments. Correct call: start.sh <dirname> <numclients> <offset> <logfile>"
else

	xactdirname=$1-xact-revised-b
	numclients=$2
	offset=$3
	logfile=$4
	echo "xact directory: " $xactdirname
	echo "starting drivers with number of clients: " $numclients
	echo "xact file offset: " $offset

	for i in $(seq 0 $(($numclients-2))); do
		echo "Starting client " $(($i + $offset))
		python2.7 driver.py $xactdirname $(($i + $offset)) $logfile&
	done
	echo "Starting client " $(($numclients-1+$offset))
	python2.7 driver.py $xactdirname $(($numclients-1 + $offset)) $logfile ;
	wait $!
	sleep 5
fi
