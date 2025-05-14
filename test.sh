sudo ./format /dev/sdd
sudo insmod lsdm.ko
sudo /sbin/dmsetup create TL1 --table '0 20971520 lsdm /dev/sdd TL1 524288 20971520'
sudo mkfs.ext4 /dev/dm-0
sudo mount -t ext4 /dev/dm-0  /mnt
