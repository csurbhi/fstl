sudo insmod lsdm.ko
#sudo /sbin/dmsetup create TL1 --table '0 15500574720 lsdm /dev/sdb TL1 524288 15500574720'
# testing for a 1TB disk only, to allow the current tm flush to work
sudo /sbin/dmsetup create TL1 --table '0 261619712 lsdm /dev/sdd TL1 524288 261619712'
sudo mkfs.ext4 /dev/dm-0
sudo mount -t ext4 /dev/dm-0 /mnt
#sudo /sbin/dmsetup create TL1 --table '0 2104492032 lsdm /dev/sda TL1 524288 2104492032'
#sudo /sbin/dmsetup create TL1 --table '0 209190912 lsdm /dev/sdb TL1 524288 209190912'
#sudo /sbin/dmsetup create TL1 --table '0 209715200 lsdm /dev/sdb TL1 524288 209715200'
#date +%s.%N
#sudo ./writezones
#date +%s.%N
