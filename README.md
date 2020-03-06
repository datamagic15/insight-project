# Edgalert

Real time alerts based on searches of the SEC's Edgar database.

## Setup
I'm using a 4 node Kafka/Zookeeper cluster to handle the streaming data
I also have a 4 node Spark/Hadoop cluster.
Another server hosts the mysql database and dash web based UI


## Introduction

## Architecture

## Dataset
I'm using a subset of the log files from the SEC's EDGAR database.

https://www.sec.gov/dera/data/edgar-log-file-data-set.html


kafka-streaming is the producer that sends data to kafka.
It takes 2 arguments

1) dev (local run) or prod( aws run )
2) The file to stream

SparkStreamingJob is the job that consumes the data and 
performs aggregations.
It takes 1 argument

1) dev (local run) or prod( aws run )

