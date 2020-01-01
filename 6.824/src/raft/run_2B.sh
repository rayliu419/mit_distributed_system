#!/Bin/Bash

rm out_2B

if [ -z "$1" ]
then
for i in $(seq 1 5)
 do
 go test -run 2B >> out_2B
  echo "================================================================" >> out_2B
  done
else
  for i in $(seq 1 5)
  do
  go test -run $1 >> out_2B
  echo "================================================================" >> out_2B
  done
fi