#!/bin/bash

rm out_all

for i in $(seq 1 5)
 do
 go test >> out_all
 echo "================================================================" >> out_all
 done
fi