#!/bin/bash
set -e
dot -Tpng -o test.png test.dot
#pkill feh || true
