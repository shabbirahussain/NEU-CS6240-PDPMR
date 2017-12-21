# README-A5 Routing

This assignment provides a list of possible two-hop flights that minimize the chance of missing a connection between a source and destination airport.

## Configuration
1. Setup hadoop
1. Configure variables in the `Makefile`:
    * HADOOP_HOME
    * IN_DIR_ON_HDFS
    * OUT_DIR_ON_HDFS
    * LOCAL_OUT_DIR
    * QUERY_FILE

## Execute (in pseudo-distributed)
```
$ make all
```
