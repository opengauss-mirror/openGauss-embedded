#!/bin/bash 

if [ ! -d "test" ]; then
  mkdir test
fi
./pressure_test ReadTest 1 P 
echo "prepare success"

