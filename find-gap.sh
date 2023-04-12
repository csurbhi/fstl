#!/bin/bash

#grep "Copied" dmesg | tr -s " " | cut -d " " -f 14,16 | sed 's/)$//' | sed 's/ /, /' > pba-len-create
# grep "write_gc_extent" dmesg | tr -s  " " | cut -d " " -f 12,15 | sed -s 's/ /, /' > pba-len-final 

filen=$1
lines=`cat $filen | wc -l`
totallen=0
sum=0
prevpba=0
prevlen=0
totaldiff=0
totallen=0
for (( i=1; i<=$lines; i++ ))
do
	sum=`expr $prevpba + $prevlen`
	fields=`tail +$i $filen | head -1`
	pba=`echo $fields|tr -s " " | cut -d "," -f 1`
	len=`echo $fields|tr -s " " | cut -d "," -f 2`
	prevpba=$pba
	prevlen=$len
	totallen=`expr $len + $totallen`
	if [[ $i -gt 1 ]]
	then
		if [[ $sum -eq $pba ]]
		then
			continue;
		else
			diff=`expr $pba - $sum`
			totaldiff=`expr $totaldiff + $diff`
			echo "Gap found at entry nr $i $pba $len. Sum: $sum Diff: $diff Prev pba: $prevpba Prev len: $prevlen"
		fi
	fi
	
done
echo "Total len: $totallen"
echo "Total diff: $totaldiff"
