sudo ./getstats.sh &
STATS=$!
echo "ID: $STATS"
sleep 2
kill $STATS
killall iostat
