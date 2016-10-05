#!/bin/bash

if [ "$#" -ne 4 ];
then
	echo "wrong number of arguments. Correct call: start.sh <dirname> <numclients> <replicationfactor> <logfile>"
else

	datadirname=$1-data
	xactdirname=$1-xact-revised-b
	numclients=$2
	replication=$3
	logfile=$4
	echo "data directory: " $datadirname
	echo "xact directory: " $xactdirname
	echo "starting drivers with number of clients: " $numclients
	echo "replication factor: " $replication
	echo "changing keyspace replication to $replication on cassandra node"
	cqlsh -e "alter keyspace cs4224 with replication = {'class': 'SimpleStrategy', replication_factor' : $replication}"
	echo "loading directory"
	python2.7 load_data.py $datadirname ;

	for i in $(seq 0 $(($numclients-2))); do
		echo "Starting client " $i
		python2.7 driver.py $xactdirname $i $logfile&
	done
	echo "Starting client " $(($numclients-1))
	python2.7 driver.py $xactdirname $(($numclients-1)) $logfile ;
	wait $!
	sleep 5
fi
