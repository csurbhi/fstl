#!/bin/bash

sudo insmod lsdm.ko
sudo ./format 
sudo python setupdm.py 
sudo ./writezones
echo ""
sudo ./readverify
sudo ./readverify
sleep 1
sudo dmsetup remove TL0
sudo rmmod lsdm 
