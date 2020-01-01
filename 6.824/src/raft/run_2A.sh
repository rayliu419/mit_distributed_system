#!/bin/bash

rm out_2A

if [ -z "$1" ]
then
for i in $(seq 1 5)
 do
 go test -run 2A >> out_2A
  echo "================================================================" >> out_2A
  done
else
  for i in $(seq 1 5)
  do
  go test -run $1 >> out_2A
  echo "================================================================" >> out_2A
  done
fi