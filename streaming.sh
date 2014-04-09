#!/bin/bash

HADOOP_HOME=/usr/local/Cellar/hadoop/2.3.0/bin/hadoop
JAR=/Users/suman/Downloads/hadoop-streaming-0.20.203.0.jar

HSTREAMING="$HADOOP_HOME jar $JAR"

$HSTREAMING \
 -mapper  'ruby map.rb' \
 -reducer 'ruby reducer1.rb' \
 -file map.rb \
 -file reducer1.rb \
 -input '/Users/suman/Codebase/s3_to_riak/setup.sh' \
 -output /Users/suman/Codebase/s3_to_riak/output