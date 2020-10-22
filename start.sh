#sudo insmod dm-nstl.ko
sudo python3 format-stl.py /dev/vdb 524288 2 75 75
sudo python3 seagate.py TL1 /dev/vdb fifo
sudo mkfs.ext4 /dev/dm-0
