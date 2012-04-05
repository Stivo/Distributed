#!/bin/bash
set -e
cp dsl/src/main/scala/ch/epfl/distributed/datastruct/*.scala spark/src/main/scala/ch/epfl/distributed/datastruct/
cp dsl/src/main/scala/ch/epfl/distributed/datastruct/*.scala scoobi/src/main/scala/ch/epfl/distributed/datastruct/
dot -Tpng -o test.png test.dot
#pkill feh || true
