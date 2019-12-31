#!/bin/bash

rm out_2A
for i in $(seq 1 5)  
do
go test -run 2A >> out_2A
echo "================================================================" >> out_2A
done
