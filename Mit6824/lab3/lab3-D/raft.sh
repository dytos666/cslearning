#!/bin/bash
for i in {1..10}
do
    echo "===== Run $i ====="
    GMAXPROCS=4   go test -race 
done

