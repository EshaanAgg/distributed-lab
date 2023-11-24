#!/bin/bash

go run main.go ii master sequential data/pg-*.txt
sort -k1,1 tmp/mrtmp.iiwc | sort -snk2,2 tmp/mrtmp.iiwc | grep -v '16' | tail -10 | diff - test/ii-testout.txt > test/diff.out
if [ -s test/diff.out ]
then
echo "Failed test. Output should be as in ii-testout.txt. Your output differs as follows (from diff.out):" > /dev/stderr
  cat test/diff.out
else
  echo "Passed test" > /dev/stderr
fi

