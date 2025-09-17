#!/bin/bash
for i in {1..40}
do
    echo "===== Run $i ====="
    GOMAXPROCS=4  time go test -race -run 3B 
done

