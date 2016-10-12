#!/bin/bash

if [ "$#" -ne 4 ];
then
	echo "wrong number of arguments. Correct call: start.sh <dirname> <numclients> <replicationfactor> <logfile>"
else

	xactdirname=$1-xact-revised-b
	numclients=$2
	logfile=$3
	echo "xact directory: " $xactdirname
	echo "starting drivers with number of clients: " $numclients

	for i in $(seq 0 $(($numclients-2))); do
		echo "Starting client " $i
		python2.7 driver.py $xactdirname $i $logfile&
	done
	echo "Starting client " $(($numclients-1))
	python2.7 driver.py $xactdirname $(($numclients-1)) $logfile ;
	wait $!
	sleep 5
fi
