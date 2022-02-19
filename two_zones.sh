#!/bin/bash

sudo insmod lsdm.ko
sudo ./format 
sudo python setupdm.py 
sudo ./writezones
sudo ./readverify
#sudo dmsetup remove TL0
#sudo rmmod lsdm 
