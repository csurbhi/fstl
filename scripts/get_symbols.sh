sudo insmod dm-nstl.ko
text=`sudo cat /sys/module/dm_nstl/sections/.text`
data=`sudo cat /sys/module/dm_nstl/sections/.data`
bss=`sudo cat /sys/module/dm_nstl/sections/.bss`
echo "add-symbol-file ~/github/fstl/dm-nstl.ko $text -s .data $data -s .bss $bss"
#sudo python ./setupdm.py
