#!/bin/bash


for data in D8 D40
do
	replication=3
	for numClients in 10 20 40	
	do
		echo "Running benchmark for $data with keyspace replication: $replication and numClients: $numClients"			./start.sh $data $numClients $replication > $data-r$replication-$numClients.txt ;
	done
done