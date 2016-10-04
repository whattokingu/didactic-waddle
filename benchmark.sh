#!/bin/bash


for data in D8 D40
do
	for replication in 1 3
	do
		for numClients in 10 20 40
		do
			./start.sh $data $numClients $replication > $data-r$replication-$numClients.txt