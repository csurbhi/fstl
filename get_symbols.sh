MNAME="lsdm"
sudo insmod $MNAME.ko
text=`sudo cat /sys/module/$MNAME/sections/.text`
data=`sudo cat /sys/module/$MNAME/sections/.data`
bss=`sudo cat /sys/module/$MNAME/sections/.bss`
echo "add-symbol-file ~/github/fstl/$MNAME.ko $text -s .data $data -s .bss $bss"
#sudo python ./setupdm.py
