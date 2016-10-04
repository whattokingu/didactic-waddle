#!/bin/bash

if [ "$#" -ne 3 ];
then
	echo "wrong number of arguments. Correct call: start.sh <dirname> <numclients> <replicationfactor>"
else

	datadirname=$1-data
	xactdirname=$1-xact-revised-b
	numclients=$2
	replication=$3
	echo "data directory: " $datadirname
	echo "xact directory: " $xactdirname
	echo "starting drivers with number of clients: " $numclients
	echo "replication factor: " $replication
	echo "changing keyspace replication to $replication on cassandra node"
	cqlsh -e "alter keyspace cs4224 with replication = {'class': 'SimpleStrategy', replication_factor' : $replication}"
	echo "loading directory"
	python load_data.py $datadirname ;

	for i in $(seq 0 $(($numclients-1))); do
		echo "Starting client " $i
		python driver.py $xactdirname $i &
	done
fi
