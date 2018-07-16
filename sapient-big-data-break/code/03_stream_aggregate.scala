#Usage:
#spark-shell --conf "spark.driver.extraClassPath=$SPARK_HOME/jars/kafka-clients-1.1.0.jar"

#Remove redundant folder from earlier run
#rm -rf /u01/workspace/output1

val df = spark.read.
  format("csv").
  option("header", "false").
  load("/tmp/spark_test.csv").
  select($"_c0".alias("DT_HR_HH"), $"_c1".alias("SUM"), $"_c2".alias("COUNT"))

val df1 = df.
  withColumn("DT", split(col("DT_HR_HH"), "\\_").getItem(0)).
  withColumn("HR", split(col("DT_HR_HH"), "\\_").getItem(1)).
  withColumn("HH1", split(col("DT_HR_HH"), "\\_").getItem(2)).
  withColumn("HH2", split(col("DT_HR_HH"), "\\_").getItem(3)).
  select("DT", "HR", "HH1", "HH2", "SUM", "COUNT")

val df2 = df1.select(lpad($"HH1", 2, "0").as("HH1"),
                     lpad($"HH2", 2, "0").as("HH2"),
                     $"DT",
                     $"HR",
                     $"SUM",
                     $"COUNT")

val df3 = df2.
  groupBy("HH1", "HH2", "DT", "HR").
  agg(sum("SUM") / sum("count")).as("AVG1").
  sort("HH1", "HH2", "DT", "HR")

df3.
  coalesce(1).
  write.
  format("com.databricks.spark.csv").
  option("header", "false").
  save("/u01/workspace/output2")

