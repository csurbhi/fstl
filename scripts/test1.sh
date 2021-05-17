sudo insmod dm-nstl.ko
sudo ./format | tee ./format_output
sudo python setupdm.py
sudo dd if=./0001-debug-contd.patch of=/dev/dm-0 
sudo dd if=/dev/dm-0 of=/tmp/test1.patch count=1 bs=5115
diff /tmp/test1.patch ./0001-debug-contd.patch
#sudo dmsetup remove TL0
#sudo rm /tmp/test1.patch
#sudo python setupdm.py
#sudo dd if=/dev/dm-0 of=/tmp/test1.patch count=1 bs=5115
#diff /tmp/test1.patch ./0001-debug-contd.patch
#sudo rmmod dm_nstl
