#!/bin/bash

hours=0
mins=0
seconds=0
nano=0

function gettimediff() 
{
	echo "hours1: $hours1"
	echo "mins1: $mins1"
	echo "seconds1: $seconds1"
	echo "nano1: $nano1"



	echo "hours2: $hours2"
	echo "mins2: $mins2"
	echo "seconds2: $seconds2"
	echo "nano2: $nano2"
	if test $nano2 -lt $nano1
	then
		$nano2=$((nano2 + 1000000000))
		if test $seconds2 -gt 0
		then
			seconds2=$((seconds2 - 1))
		else
			seconds2=59
			if test $mins2 -gt 0
			then
				mins2=$((mins2 - 1))
			else
				mins2=59
				if test $hours2 -gt 0
				then
					hours2=$((hours2 - 1))
				else
					echo "error in calculation "
				fi
			fi
		fi
	fi


	if test $seconds2 -lt $seconds1
	then
		seconds2=$((seconds2 + 1))
		if test $mins2 -gt 0
		then
			mins2=$((mins2 - 1))
		else
			mins2=59
			if test $hours2 -gt 0
			then
				hours2=$((hours2 - 1))
			else
				echo "error in calculation "
			fi
		fi
	fi

	if test $mins2 -lt $mins1
	then
		mins2=$((mins2 + 1))
		if test $hours2 -gt 0
		then
			hours2=$((hours2 - 1))
		else
			echo "error in calculation "
		fi
	fi
	hours=$((hours2 - hours1))
	mins=$((mins2 - mins1))
	seconds=$((seconds2 - seconds1))
	nano=$((nano2 - nano1))
	echo "time difference: $hours:$mins:$seconds:$nano"
}

function addtime()
{
	time1=$1
	time2=$2

	nano=$(( nano1 + nano2 ))
	if test $nano -gt 1000000000
	then
		seconds2=$(( seconds2 + 1 ))
		nano=$(( nano - 1 ))
	fi

	seconds=$((seconds1 + seconds2 ))
	if test $seconds -gt 60
	then
		mins2=$(( mins2, 1 ))
		seconds=$(( seconds - 1 ))
	fi

	mins=$(( mins1 + mins2 ))
	if test $mins -gt 60
	then
		hours=$(( hours + 1 ))
		mins=$(( mins - 1 ))
	fi

	hours=$(( hours1 + hours2 ))

	echo "time addition: $hours:$mins:$seconds:$nano"
}


time1=`date "+%k:%_M:%_S:%_N"`
sleep 1;
time2=`date "+%k:%_M:%_S:%_N"`

echo "time1: $time1"
echo "time2: $time2"
hours1=`echo $time1 | cut -d ":" -f 1`
mins1=`echo $time1 | cut -d ":" -f 2`
seconds1=`echo $time1 | cut -d ":" -f 3`
nano1=`echo $time1 | cut -d ":" -f 4`


hours2=`echo $time2 | cut -d ":" -f 1`
mins2=`echo $time2 | cut -d ":" -f 2`
seconds2=`echo $time2 | cut -d ":" -f 3`
nano2=`echo $time2 | cut -d ":" -f 4`


gettimediff $time1 $time2

hours1=`echo $time1 | cut -d ":" -f 1`
mins1=`echo $time1 | cut -d ":" -f 2`
seconds1=`echo $time1 | cut -d ":" -f 3`
nano1=`echo $time1 | cut -d ":" -f 4`


hours2=`echo $time2 | cut -d ":" -f 1`
mins2=`echo $time2 | cut -d ":" -f 2`
seconds2=`echo $time2 | cut -d ":" -f 3`
nano2=`echo $time2 | cut -d ":" -f 4`

addtime $time1 $time2
