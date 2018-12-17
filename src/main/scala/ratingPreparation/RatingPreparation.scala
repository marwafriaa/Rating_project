package ratingPreparation
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import utils.Util

class RatingPreparation {

  def calculate_rating(util : Util, cleaned_id_df : DataFrame): DataFrame ={
    cleaned_id_df.createOrReplaceTempView("rating")

    val rating_df = calculate_new_rating(util,cleaned_id_df)
    rating_df
  }


  def calculate_new_rating(util : Util,df : DataFrame): DataFrame ={
    df.schema
    var max_rating = util.spark.sql("SELECT max(timestamp) as ts FROM rating  ").first.getString(0)

    val new_column = when((col("timestamp") - BigInt(max_rating))/60 >= 1, col("rating") * 0.099 * ((col("timestamp") - BigInt(max_rating) )/60))
    val new_rating = df.withColumn("rating", new_column)
    new_rating
    //df
  }
  def calculate_rating_sum(df: DataFrame,util : Util): DataFrame ={
    val aggdf = df.groupBy("userId", "itemId").agg(sum( "rating")).as("ratingSum")
    aggdf

  }
}
