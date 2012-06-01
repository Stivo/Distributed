#!/bin/bash
set -e
cp dsl/src/main/resources/*.scala spark/src/main/scala/ch/epfl/distributed/datastruct/
cp dsl/src/main/resources/*.scala scoobi/src/main/scala/ch/epfl/distributed/datastruct/
#pkill feh || true

for x in $(find | grep .dot$)
do
y=${x/.dot/}
echo "Updating png for $y.dot"
dot -Tpng $y.dot > $y.png
done;
