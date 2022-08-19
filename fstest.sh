sudo ./format
sudo ./create.sh
sudo mkfs.ext4 /dev/dm-0
sudo mount -t ext4 /dev/dm-0 /mnt
sudo ./writezones
sleep 10
free -h
sudo ./readverify
free -h
sudo umount /mnt
sudo dmsetup remove TL1
sudo rmmod lsdm.ko
