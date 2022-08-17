sudo insmod lsdm.ko
#sudo /sbin/dmsetup create TL1 --table '0 15500574720 lsdm /dev/sdb TL1 524288 15500574720'
# testing for a 1TB disk only, to allow the current tm flush to work
sudo /sbin/dmsetup create TL1 --table '0 2091909120 lsdm /dev/sdb TL1 524288 2091909120'
date +%s.%N
sudo ./writezones
date +%s.%N
