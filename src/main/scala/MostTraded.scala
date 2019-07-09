import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{max, _}
import org.apache.spark.sql.types.{DoubleType, LongType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

object MostTraded {
val spark = org.apache.spark.sql.SparkSession.builder
    .getOrCreate

	def main(args: Array[String]) {

import spark.implicits._

val outputDir = "/Users/anishjoshi/Downloads/SparkProject/tester/"

val runner: Dataset[Row] = spark.read
    .option("header", value = true)
    .csv(s"/Users/anishjoshi/Downloads/SparkProject/data/local/*/*.csv")
    .drop("ISIN", "Mnemonic", "SecurityType", "Currency", "StartPrice", "MinPrice", "EndPrice", "NumberOfTrades")

val y = runner.withColumn("tradeamt",expr("MaxPrice * TradedVolume as tradeamt")).groupBy("SecurityID", "date").agg(sum("tradeamt").as("tradeamt")).orderBy(desc("date"),asc("tradeamt"))
val x = y.groupBy("date").agg(max("tradeamt").as("maxtradeamt"))
val z = x.join(y, x("maxtradeamt") <=> y("tradeamt") && x("date") <=> y("date"))
val outputResult = z.orderBy(desc("maxtradeamt")).repartition(1)
  outputResult.rdd.saveAsTextFile(outputDir)
}
}