package utils

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

class Util {

  val conf = new SparkConf().setMaster("local[*]").setAppName("ratingCalculation")
  // Create a Scala Spark Context.
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  val spark=sqlContext.sparkSession

}
