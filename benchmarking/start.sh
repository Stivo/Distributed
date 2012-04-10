
OUTPUT=./output
INPUTS="/home/stivo/master/testdata/pagecounts $OUTPUT"
INPUTS="/home/stivo/master/testdata/tpch/ 50000 1997-06-04 $OUTPUT"
INPUTS="/home/stivo/master/testdata/currenttmp-5m $OUTPUT"

./warmup.sh
./run.sh $INPUTS
du -h output/
rm -rf output
./warmup.sh
./runspark.sh $INPUTS
du -h output/
rm -rf output
#./compare.py
