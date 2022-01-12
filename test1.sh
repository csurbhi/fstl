#!/bin/bash

sudo insmod lsdm.ko
sudo ./format  > /dev/null 
sudo python setupdm.py > /dev/null
for i in {1..1}
do
	sudo dd if=scripts/0001-debug-contd.patch of=/dev/dm-0 
done
sudo dd if=/dev/dm-0 of=/tmp/test1.patch bs=4096 count=2
diff /tmp/test1.patch scripts/0001-debug-contd.patch
#sudo dmsetup remove TL0
#sudo rm /tmp/test1.patch
#sudo python setupdm.py
#sudo dd if=/dev/dm-0 of=/tmp/test1.patch count=1 bs=5115
#diff /tmp/test1.patch scripts/0001-debug-contd.patch
#sudo dmsetup remove TL0
#sudo rmmod lsdm 
