import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{max, _}
import org.apache.spark.sql.types.{DoubleType, LongType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

object ImpliedVolume {
val spark = org.apache.spark.sql.SparkSession.builder
    .getOrCreate

	def main(args: Array[String]) {

import spark.implicits._

val outputDir = "Enter Output Path Here"

val runner: Dataset[Row] = spark.read
    .option("header", value = true)
    .csv(s"Enter Input Path Here/*/*.csv")
    .drop("ISIN", "Mnemonic", "SecurityType", "Currency", "StartPrice", "EndPrice", "NumberOfTrades", "TradedVolume")

val temp = runner.withColumn("implied_volume", expr("(MaxPrice - MinPrice) / MinPrice as implied_volume")).groupBy("date","SecurityID","SecurityDesc")
val outputResult: DataFrame = temp.agg(avg("implied_volume").as("implied_volume")).orderBy("implied_volume","date").repartition(1)
  outputResult.rdd.saveAsTextFile(outputDir)
}
}
