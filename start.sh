#!/bin/bash

numclients=$2
dirname=$1
echo "starting drivers with number of clients: " $numclients


for i in $(seq 0 $(($numclients-1))); do
	echo "Starting client " $i
	python driver.py $dirname $i &
done