#!/bin/bash

for i in $(seq 1 5)  
do   
go test -run 2B >> out
echo "================================================================" >> out
done
