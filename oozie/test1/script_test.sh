#!/bin/bash

echo 'spark test'

sleep 2

spark2-shell -i /home/shjeong/spark_test.scala

sleep 5

echo 'Hello World!'

exit 0
