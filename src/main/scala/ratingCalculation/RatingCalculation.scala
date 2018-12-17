package ratingCalculation

import utils.Util
import idGeneration.IdGeneration
object RatingCalculation {

  def main(args: Array[String]): Unit = {
    var util = new Util()
    var idg = new IdGeneration

    val cleaned_id_df = idg.generate_new_id_integers(util,args(0))
    cleaned_id_df.show()
    //val res2 = calculate_rating(util,cleaned_id_df)
    //res2.show()





  }


}
