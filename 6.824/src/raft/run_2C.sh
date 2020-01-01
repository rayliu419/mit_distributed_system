#!/bin/bash

rm out_2C

if [ -z "$1" ]
then
for i in $(seq 1 5)
 do
 go test -run 2C >> out_2C
  echo "================================================================" >> out_2C
  done
else
  for i in $(seq 1 5)
  do
  go test -run $1 >> out_2C
  echo "================================================================" >> out_2C
  done
fi