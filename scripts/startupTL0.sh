#!/bin/bash

lsmod | grep nstl
sudo rmmod dm_nstl
sudo insmod dm-nstl.ko
sudo ./format
sudo python setupdm.py

