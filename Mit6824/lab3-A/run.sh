#!/bin/bash
for i in {1..20}
do
    echo "===== Run $i ====="
    GOMAXPROCS=4 go test -race -run 3A
done

