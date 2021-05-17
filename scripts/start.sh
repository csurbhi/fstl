sudo insmod dm-nstl.ko
sudo python format-stl.py /dev/sdb 524288 2 118 28000
sudo python seagate.py TL1 /dev/sdb fifo
