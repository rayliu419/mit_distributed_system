#!/bin/bash

rm out*

for i in $(seq 1 5)
do
go test -run $1 >> out_$1
echo "================================================================" >> out_$1
done
