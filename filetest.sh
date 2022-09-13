sudo ./format | tee ./format.txt
sudo insmod lsdm.ko
sudo ./create.sh
sudo ./writezones
free -h
sudo ./readverify
free -h
sudo mv /mnt/test /mnt/test.1
sudo ./writezones
free -h
sudo ./readverify
free -h
