# goit-de-fp

# Part 1

spark-submit \
  --jars mysql-connector-j-8.0.32.jar \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  --conf "spark.driver.extraClassPath=mysql-connector-j-8.0.32.jar" \
  --conf "spark.executor.extraClassPath=mysql-connector-j-8.0.32.jar" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:/path/to/log4j.properties" \
  final_streaming_job.py
