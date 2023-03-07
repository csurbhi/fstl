sudo sysctl kernel.dmesg_restrict=0
sudo ./format /dev/sdd
sudo insmod lsdm.ko
sudo /sbin/dmsetup create TL1 --table '0 25690112 lsdm /dev/sdd TL1 524288 25690112'
sudo ./writezones /dev/dm-0 1
sudo dmsetup remove TL1
sudo rmmod lsdm.ko
