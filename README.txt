How to run dm-nstl:

1- 
sudo insmod dm-nstl.ko
sudo hdparm -a256 -A1 -W1 $1 #/dev/sda
sudo ./hdparm-smr/hdparm --please-destroy-my-drive --reset-all-write-pointers $1 #/dev/sda
##sudo python2.7 format-stl.py $1 524288 2 256 28000 #/dev/sda 524288 2 10 400
##sudo python2.7 seagate_full_extent.py $2 $1 $3
sudo python2.7 format-stl.py $1 524288 2 118 28000
sudo python2.7 seagate.py $2 $1 $3

