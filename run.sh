#!/bin/bash
/usr/local/hadoop/bin/hdfs dfs -rm -r -f /home/ubuntu/MapReduce_Triangle_Counting/output
/usr/local/hadoop/bin/hdfs dfs -rm -r -f /home/ubuntu/MapReduce_Triangle_Counting/temp/output-first-mapreduce
/usr/local/hadoop/bin/hdfs dfs -rm -r -f /home/ubuntu/MapReduce_Triangle_Counting/temp/output-second-mapreduce
rm TriangleCounting.jar
rm TriangleCounting*.class

/usr/local/hadoop/bin/hadoop com.sun.tools.javac.Main TriangleCounting.java
jar cf TriangleCounting.jar TriangleCounting*.class

/usr/local/hadoop/bin/hadoop jar triplet.jar TriangleCounting $1 $2
/usr/local/hadoop/bin/hdfs dfs -cat /home/ubuntu/MapReduce_Triangle_Counting/output/*