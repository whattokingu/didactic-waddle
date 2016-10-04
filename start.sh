#!/bin/bash

if [ "$#" -ne 3 ];
then
	echo "wrong number of arguments. Correct call: start.sh <dirname> <numclients> <replicationfactor>"
else
	dirname=$1
	numclients=$2
	replication=$3
	echo "starting drivers with number of clients: " $numclients
	echo "replication factor: " $replication
	cqlsh -e "alter keyspace cs4224 with replication = {'class': 'SimpleStrategy', replication_factor' : $replication}"

	for i in $(seq 0 $(($numclients-1))); do
		echo "Starting client " $i
		python driver.py $dirname $i &
	done
fi
