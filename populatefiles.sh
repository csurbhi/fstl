#!/bin/bash

for i in {1..10}
do
	fname="/mnt/test$i"
	nrzones=10
	./writezones  $fname $nrzones
	nrzones=$(( nrzones + 10 ))
done
