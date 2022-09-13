grep print_sub_extents dmesg | tr -s " "  | cut -d " " -f 12 | sed 's/,*$//g' > extent.pba
