#!/bin/bash
file= "input.sh"
hdfs dfs -test -e /user/HadoopSparkProjectPMN/$file

if [ -z "$1" ]
then
  echo "no contents in input"
else
  echo "content exists"