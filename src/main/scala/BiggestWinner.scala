import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{max, _}
import org.apache.spark.sql.types.{DoubleType, LongType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

object BiggestWinner {
val spark = org.apache.spark.sql.SparkSession.builder
    .getOrCreate

	def main(args: Array[String]) {

import spark.implicits._

val outputDir = "Enter Output Path Here"
val runner: Dataset[Row] = spark.read
    .option("header", value = true)
    .csv(s"Enter Input Path Here/*/*.csv")
    .drop("ISIN", "Mnemonic", "SecurityType", "Currency", "MaxPrice", "MinPrice", "TradedVolume", "NumberOfTrades")


val maxWindow = Window.partitionBy("securityID", "date").orderBy($"time".desc)
val minWindow = Window.partitionBy("securityID", "date").orderBy($"time".asc)

val maximum = runner.withColumn("row", row_number.over(maxWindow)).where($"row" === 1).drop("row").drop("StartPrice")
val minimum = runner.withColumn("row", row_number.over(minWindow)).where($"row" === 1).drop("row").drop("EndPrice")


val temp: DataFrame = minimum.join(maximum, Seq("securityID", "date")).drop(maximum("SecurityDesc"))
    .withColumn("winpercent", expr("(EndPrice - StartPrice) / StartPrice as winpercent"))
    .drop("StartPrice", "EndPrice", "time")

val outputResult: DataFrame = temp.groupBy("date").agg(max("winpercent").as("winpercent"))
    .join(temp, Seq("date", "winpercent"), "left").repartition(1).orderBy("date")
  outputResult.rdd.saveAsTextFile(outputDir)
}
}
