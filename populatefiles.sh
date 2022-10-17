#!/bin/bash

hours=0
mins=0
seconds=0
nano=0

function gettimediff() 
{

	time1="$1"
	time2="$2"
	outfile=$5

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
	echo "Action: $3 " >> $outfile
	echo "Action: $3 " 
	echo "time taken to $3 $4 zones was: $hours:$mins:$seconds:$nano" >> $outfile
	echo "time taken to $3 $4 zones was: $hours:$mins:$seconds:$nano"
}

# on herambh, sda - host managed, sdb - host aware

sudo ./format
sudo insmod lsdm.ko 
#sudo /sbin/dmsetup create TL1 --table '0 2104492032 lsdm /dev/sda TL1 524288 2104492032'
sudo /sbin/dmsetup create TL1 --table '0 128450560 lsdm /dev/sda TL1 524288 128450560'
#sudo mkfs.ext4 /dev/dm-0
#sudo mount -t ext4 /dev/dm-0 /mnt
nrzones=$1
out=$2
DIR="zones/$out/$nrzones"
mkdir -p $DIR
outfile="$DIR/$nrzones.out"
touch $outfile
for i in {1..1}
do
	echo "-----------------------------" >> $DIR/time.out
	echo "-----------------------------" 
	#fname="/mnt/test$i"
	fname="/dev/dm-0"
	echo "Beginning round: $i" > $outfile
	echo "Beginning round: $i"
	iostat -d -h -N -x -y sda 1 > $DIR/sda_iostats.out.before &
	IOPID=$!
	dstat -d -c -g -m -i  -r -t > $DIR/sda_dstats.out.before &
	DPID=$!
	time1=`date "+%k:%-M:%-S:%-N"`
	./writezones  $fname $nrzones | tee "$DIR/writezones.out.$nrzones"
	time2=`date "+%k:%-M:%-S:%-N"`
	kill $IOPID
	kill $DPID

	hours1=`echo $time1 | cut -d ":" -f 1`
	mins1=`echo $time1 | cut -d ":" -f 2`
	seconds1=`echo $time1 | cut -d ":" -f 3`
	nano1=`echo $time1 | cut -d ":" -f 4`


	hours2=`echo $time2 | cut -d ":" -f 1`
	mins2=`echo $time2 | cut -d ":" -f 2`
	seconds2=`echo $time2 | cut -d ":" -f 3`
	nano2=`echo $time2 | cut -d ":" -f 4`
	echo "Time at the beginning: $time1"  >> $outfile
	echo "Time after test completed: $time2" >> $outfile
	echo "Time at the beginning: $time1"
	echo "Time after test completed: $time2"
	gettimediff $time1 $time2 "write-read-overwrite-read" $nrzones $outfile
	nrzones=$(( nrzones * 2 ))
done
#sudo umount /mnt
sudo dmsetup remove TL1
sudo rmmod lsdm.ko
echo "-----------------------------" >> $DIR/time.out
echo "-----------------------------"
exit 0;
