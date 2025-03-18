#!/bin/bash 

if [ ! -d "test" ]; then
  mkdir test
fi
nohup ./pressure_test ReadTest 8 R > nohup.out 2>&1 &

