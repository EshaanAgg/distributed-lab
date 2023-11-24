#!/bin/bash

go run main.go mapreduce master sequential data/pg-*.txt
sort -n -k2 tmp/mrtmp.wcseq | tail -10 | diff - test/mr-testout.txt > test/diff.out
if [ -s test/diff.out ]
then
echo "Failed test. Output should be as in mr-testout.txt. Your output differs as follows (from diff.out):" > /dev/stderr
  cat test/diff.out
else
  echo "Passed test" > /dev/stderr
fi

