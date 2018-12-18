package idGeneration
import utils.Util
import org.apache.spark.sql.functions.{array, monotonically_increasing_id, row_number, sum}
import org.apache.spark.sql.DataFrame


/* Add new id generated incrementaly (Integer)
 * input : Data frame
  * output : 2 Dataframes : the first for users , second one for items */
class IdGeneration {

  /* This function generates the 2 dataframes with new ids as Integer */
  def generate_new_id_integers(util : Util,inputFile : String): DataFrame = {

    // set the output file path
    val outputFile = System.getProperty("user.dir")

    // Load our input data.
    val df = util.spark.read.format("csv").option("header", "false").load(inputFile)
    // Rename column names to manipulate its data easily
    val dfRenamed = df.selectExpr("_c0 as userId", "_c1 as itemId", "_c2 as rating", "_c3 as timestamp")

    // Generate idAsInteger for items
    val item_new_format = generate_new_integer_id_items(dfRenamed)
    // Generate file for results
    generate_file_for_results(outputFile + "lookup_product.csv",item_new_format)

    //Generate new id for users
    val user_new_format = generate_new_integer_id_users(dfRenamed)
    // Generate file for results
    generate_file_for_results(outputFile + "lookupuser.csv",user_new_format)


    //update input data with the new generated ids
    val final_res = generate_main_data_new_ids(user_new_format,item_new_format,dfRenamed)
    // return the final new structure
    final_res

  }

  /* This function generates new items with ids as Integer */
  def generate_new_integer_id_items(dfRenamed:DataFrame): DataFrame ={
    // Generate new id for items
    val gbitem = dfRenamed.groupBy(col1 = "itemId").count().withColumn("itemIdAsInteger", monotonically_increasing_id())

    // eliminate count column to get the desired structure
    val gbitem_f = gbitem.select("itemId", "itemIdAsInteger")

    gbitem_f

  }

  /* This function generates new users with ids as Integer */
  def generate_new_integer_id_users(dfRenamed:DataFrame): DataFrame ={
    val gbuser = dfRenamed.groupBy(col1 = "userId").count().withColumn("userIdAsInteger", monotonically_increasing_id())


    // eliminate count column to get the desired structure
    val gbuser_f = gbuser.select("userId", "userIdAsInteger")

    // return result
    gbuser

  }

  /* This function inserts data into output csv file */
  def generate_file_for_results(outputFile:String,df : DataFrame): Unit ={
    // Insert First item new structure
    df.write.format("com.databricks.spark.csv").save(outputFile)
  }

  /* This function gupdates main data with new ids */
  def generate_main_data_new_ids(gbuser:DataFrame,gbitem:DataFrame,dfRenamed:DataFrame):DataFrame={
    // update the input structure with the new ids
    val basejoinuserdf = dfRenamed.join(gbuser, dfRenamed("userId") === gbuser("userId")).select(gbuser("userIdAsInteger"), dfRenamed("itemId"), dfRenamed("rating"), dfRenamed("timestamp"))
    val basejoinitemdf = basejoinuserdf.join(gbitem, basejoinuserdf("itemId") === gbitem("itemId")).select(basejoinuserdf("userIdAsInteger"), gbitem("itemIdAsInteger"), basejoinuserdf("itemId"), basejoinuserdf("rating"), basejoinuserdf("timestamp"))
    basejoinitemdf
  }


}
