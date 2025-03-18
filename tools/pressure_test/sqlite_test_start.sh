#!/bin/bash 

if [ ! -d "test" ]; then
  mkdir test
fi
nohup ./sqlite_test > nohup.out 2>&1 &
