sudo -s
insmod dm-nstl.ko
python format-stl.py /dev/sdb 524288 2 118 28000
python seagate.py TL1 /dev/sdb fifo
