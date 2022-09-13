#!/bin/bash

hours=0
mins=0
seconds=0
nano=0

function gettimediff() 
{

	time1="$1"
	time2="$2"

	hours1=`echo $time1 | cut -d ":" -f 1`
	mins1=`echo $time1 | cut -d ":" -f 2`
	seconds1=`echo $time1 | cut -d ":" -f 3`
	nano1=`echo $time1 | cut -d ":" -f 4`


	hours2=`echo $time2 | cut -d ":" -f 1`
	mins2=`echo $time2 | cut -d ":" -f 2`
	seconds2=`echo $time2 | cut -d ":" -f 3`
	nano2=`echo $time2 | cut -d ":" -f 4`

	if test $nano2 -lt $nano1
	then
		nano2=$((nano2 + 1000000000))
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
					hours2=23
				fi
			fi
		fi
	fi


	if test $seconds2 -lt $seconds1
	then
		seconds2=$((seconds2 + 60))
		if test $mins2 -gt 0
		then
			mins2=$((mins2 - 1))
		else
			mins2=59
			if test $hours2 -gt 0
			then
				hours2=$((hours2 - 1))
			else
				hours2=23
			fi
		fi
	fi

	if test $mins2 -lt $mins1
	then
		mins2=$((mins2 + 60))
		if test $hours2 -gt 0
		then
			hours2=$((hours2 - 1))
		else
			hours2=23
		fi
	fi
	hours=$((hours2 - hours1))
	mins=$((mins2 - mins1))
	seconds=$((seconds2 - seconds1))
	nano=$((nano2 - nano1))
	echo "Action: $3 "
	echo "time taken to $3 $4 zones was: $hours:$mins:$seconds:$nano"
}

function addtime()
{
	time1=$1
	time2=$2

	nano=$(( nano1 + nano2 ))
	if test $nano -gt 1000000000
	then
		seconds2=$(( seconds2 + 1 ))
		nano=$(( nano -  1000000000 ))
	fi

	seconds=$((seconds1 + seconds2 ))
	if test $seconds -gt 60
	then
		mins2=$(( mins2 + 1 ))
		seconds=$(( seconds - 60 ))
	fi

	mins=$(( mins1 + mins2 ))
	if test $mins -gt 60
	then
		hours=$(( hours + 1 ))
		mins=$(( mins - 60 ))
	fi

	hours=$(( hours1 + hours2 ))

	echo "time addition: $hours:$mins:$seconds:$nano"
}


nrzones=$1
echo "Set NRZONES: $nrzones"

DIR="zones/alternate_blocks/$nrzones"
mkdir -p $DIR

hours1=`echo $time1 | cut -d ":" -f 1`
mins1=`echo $time1 | cut -d ":" -f 2`
seconds1=`echo $time1 | cut -d ":" -f 3`
nano1=`echo $time1 | cut -d ":" -f 4`


hours2=`echo $time2 | cut -d ":" -f 1`
mins2=`echo $time2 | cut -d ":" -f 2`
seconds2=`echo $time2 | cut -d ":" -f 3`
nano2=`echo $time2 | cut -d ":" -f 4`

addtime $time1 $time2
sudo ./reset_zones.sh $nrzones
hdparm -W 0 -A 0 --direct -F -Z /dev/sdb
iostat -d -h -N -x -y sdb 1 > $DIR/sdb_stats.out.before &
PID=$!
time1=`date "+%k:%-M:%-S:%-N"`
#sudo ./writefullzones $nrzones
sudo ./writezones $nrzones
time2=`date "+%k:%-M:%-S:%-N"`
#sudo ./readfullzones $nrzones
#time3=`date "+%k:%-M:%-S:%-N"`
kill $PID
sudo killall iostat
echo "Time at the beginning: $time1"
echo "Time after writing completes: $time2"
#echo "Time after reading completes: $time3"
gettimediff $time1 $time2 "write-read-overwrite" $nrzones
#gettimediff $time2 $time3 "read" $nrzones
exit 0
# We don't need the next explicit tests!
iostat -d -h -N -x -y sdb 1 > $DIR/sdb_stats.out.after &
PID=$!
time1=`date "+%k:%-M:%-S:%-N"`
sudo ./writefullzones $nrzones
time2=`date "+%k:%-M:%-S:%-N"`
sudo ./readfullzones $nrzones
time3=`date "+%k:%-M:%-S:%-N"`
kill $PID
sudo killall iostat
echo "(Round 2) Time at the beginning: $time1"
echo "(Round 2) Time after writing completes: $time2"
echo "(Round 2) Time after reading completes: $time3"

gettimediff $time1 $time2 "write" $nrzones
gettimediff $time2 $time3 "read" $nrzones

