#Usage:
#spark-shell --conf "spark.driver.extraClassPath=$SPARK_HOME/jars/kafka-clients-1.1.0.jar"

import org.apache.spark.sql.types.{
  StructType,
  StructField,
  StringType,
  IntegerType
};
val userSchema = new StructType().
  add("hh1", "string").
  add("hh2", "string").
  add("dt", "string").
  add("hr", "string").
  add("val1", "Double")


val csvDF = spark.readStream.
  option("sep", ",").
  schema(userSchema).
  csv("/u01/workspace/output2").
  select(concat($"hh1", lit("_"), $"hh2", lit("_"), $"hr").as("hh_hr"),
          $"dt",
          $"val1")

import Numeric.Implicits._
import org.apache.spark.sql.streaming.{
  GroupStateTimeout,
  OutputMode,
  GroupState
}

case class InputRow(hh_hr: String, dt: String, val1: Double)
case class DeviceState(hh_hr: String,
                       var count: Integer,
                       var dt: String,
                       var values: Array[Double],
                       var val1: Double)
case class OutputRow(hh_hr: String, dt: String, alert: Integer)

def mean[T: Numeric](val1s: Iterable[T]): Double =
  val1s.sum.toDouble / val1s.size

def variance[T: Numeric](val1s: Iterable[T]): Double = {
  val avg = mean(val1s)
  val1s.map(_.toDouble).map(a => math.pow(a - avg, 2)).sum / val1s.size
}

def stdDev[T: Numeric](val1s: Iterable[T]): Double = math.sqrt(variance(val1s))

def updateWithEvent(state: DeviceState, input: InputRow): DeviceState = {
  state.count += 1
  state.values = state.values ++ Array(input.val1)
  state.val1 = input.val1
  state.dt = input.dt
  state
}

def updateAcrossEvents(
    hh_hr: String,
    inputs: Iterator[InputRow],
    oldState: GroupState[DeviceState]): Iterator[OutputRow] = {
  inputs.toSeq.sortBy(_.dt).toIterator.flatMap { input =>
    val state =
      if (oldState.exists) oldState.get
      else DeviceState(hh_hr, 0, input.dt, Array(), input.val1)
    val newState = updateWithEvent(state, input)
    if (newState.count > 1) {
      oldState.update(
        DeviceState(hh_hr,
                    newState.count,
                    newState.dt,
                    newState.values,
                    newState.val1))
      val y =
        if (Math.abs(newState.val1 - mean(newState.values)) > stdDev(
              newState.values)) 1
        else 0
      Iterator(OutputRow(hh_hr, newState.dt, y))
    } else {
      oldState.update(newState)
      Iterator(OutputRow(hh_hr, newState.dt, 0))
    }
  }
}

import org.apache.spark.sql.streaming.Trigger

csvDF.
  as[InputRow].
  groupByKey(_.hh_hr).
  flatMapGroupsWithState(OutputMode.Append, GroupStateTimeout.NoTimeout)(
   updateAcrossEvents).
  writeStream.
  queryName("soln1").
  format("memory").
  outputMode("append").
  start()

val df = spark.sqlContext.sql(
  "SELECT cast(split(hh_hr,'_')[0] as int)||'_'||cast(split(hh_hr,'_')[1] as int)||'_'||split(dt,'-')[2]||'-'||split(dt,'-')[1]||'-'||split(dt,'-')[0]||'_'||cast(split(hh_hr,'_')[2] as int) as id,alert FROM soln1 order by hh_hr,dt")

df.coalesce(1).
  write.
  format("com.databricks.spark.csv").
  option("header", "true").
  save("/tmp/soln1")

