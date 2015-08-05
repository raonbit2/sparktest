#!/usr/bin/env bash

export SPARK_HOME=/home/spark

$SPARK_HOME/bin/spark-submit --class com.raon.example.pagerank.impl.SparkRDDImpl --name PageRank --master spark://node4.raonserver.com:7077 --files pagerank-simple.txt ./target/pagerank-uber-1.0-SNAPSHOT.jar
