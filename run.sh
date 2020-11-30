#!/bin/bash
/usr/local/hadoop/bin/hdfs dfs -rm -r -f /home/ubuntu/output
/usr/local/hadoop/bin/hdfs dfs -rm -r -f /home/ubuntu/first-mapreduce
/usr/local/hadoop/bin/hdfs dfs -rm -r -f /home/ubuntu/second-mapreduce
rm triplet.jar
rm ClosedTriplet*.class

/usr/local/hadoop/bin/hadoop com.sun.tools.javac.Main TriangleCounting.java
jar cf triplet.jar TriangleCounting*.class

/usr/local/hadoop/bin/hadoop jar triplet.jar TriangleCounting /home/ubuntu/input /home/ubuntu/output
/usr/local/hadoop/bin/hdfs dfs -cat /home/ubuntu/output/*

jar cf triplet.jar TriangleCounting*.class

/usr/local/hadoop/bin/hadoop jar triplet.jar TriangleCounting /home/ubuntu/input /home/ubuntu/output
/usr/local/hadoop/bin/hdfs dfs -cat /home/ubuntu/output/*