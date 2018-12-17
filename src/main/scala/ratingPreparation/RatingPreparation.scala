package ratingPreparation
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import utils.Util

class RatingPreparation {
    // This method is responsible of the final rsult and rating preparation
    def calculate_rating(util : Util, cleaned_id_df : DataFrame): DataFrame ={
    cleaned_id_df.createOrReplaceTempView("rating")
    // Rating preparation : update the rating
    val rating_df = calculate_new_rating(util,cleaned_id_df)
    // Aggregation result : the rating sum
    val final_res = calculate_rating_sum(rating_df,util)
    // return final result
    final_res
  }


  def calculate_new_rating(util : Util,df : DataFrame): DataFrame ={
    // Calculate the max rating value
    var max_rating = util.spark.sql("SELECT max(timestamp) as ts FROM rating  ").first.getString(0)
    // Verify and update the rating
    val new_column = when((col("timestamp") - BigInt(max_rating))/60 >= 1, col("rating") * 0.099 * ((col("timestamp") - BigInt(max_rating) )/60))
    val new_rating = df.withColumn("rating", new_column)
    new_rating
  }
  def calculate_rating_sum(df: DataFrame,util : Util): DataFrame ={
    // Calculate the sum value
    val aggdf = df.groupBy("userId", "itemId").agg(sum( "rating")).as("ratingSum")
    aggdf

  }
}
