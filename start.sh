#!/bin/bash

if [ "$#" -ne 5 ];
then
	echo "wrong number of arguments. Correct call: start.sh <dirname> <replication> <numclients> <offset> <logfile>"
else

	xactdirname=$1-xact-revised-b
	replication=$2
	numclients=$3
	offset=$4
	logfile=$5
	echo "xact directory: " $xactdirname
	echo "starting drivers with number of clients: " $numclients
	echo "xact file offset: " $offset
	echo "replication factor: " $replication
	echo "changing keyspace replication to $replication on cassandra node"
	cqlsh -e "alter keyspace cs4224 with replication = {'class': 'SimpleStrategy', replication_factor' : $replication}"

	for i in $(seq 0 $(($numclients-2))); do
		echo "Starting client " $(($i + $offset))
		python2.7 driver.py $xactdirname $(($i + $offset)) $logfile&
	done
	echo "Starting client " $(($numclients-1+$offset))
	python2.7 driver.py $xactdirname $(($numclients-1 + $offset)) $logfile ;
	wait $!
	sleep 5
fi
