# client
# nc -l -p 9998

# server
/usr/local/sbt/sbt package
/usr/local/spark/bin/spark-submit --class "WordCountStructuredStreaming" /usr/local/spark/mycode/streaming/target/scala-2.12/simple-project_2.12-1.0.jar