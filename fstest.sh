sudo ./format
sudo ./create.sh
sudo mkfs.ext4 /dev/dm-0
sudo mount -t ext4 /dev/dm-0 /mnt
cd /github/filebench-1.5-alpha3/
sudo ./fix
sudo ./benchmarks/db.sh
#sudo ./writezones
#free -h
#sudo ./readverify
#free -h
#sudo mv /mnt/test /mnt/test.1
#sudo ./writezones
#free -h
#sudo ./readverify
#free -h
#sudo umount /mnt
#sudo dmsetup remove TL1
#sudo rmmod lsdm.ko
