#!/bin/bash

for i in $(seq 1 5)  
do   
go test -run 2A >> out
echo "================================================================" >> out
done
