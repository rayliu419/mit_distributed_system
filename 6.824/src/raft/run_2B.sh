#!/bin/bash

rm out_2B
for i in $(seq 1 5)  
do
go test -run 2B >> out_2B
echo "================================================================" >> out_2B
done
