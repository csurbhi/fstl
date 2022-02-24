#!/bin/bash

sudo insmod lsdm.ko
sudo ./format 
sudo python setupdm.py 
sudo ./writezones
echo ""
#sudo ./readverify
echo "sleeping 10"
sleep 10
#echo "sleeping done!"
#sudo ./readverify
#sudo dmsetup remove TL0
#sudo rmmod lsdm 
