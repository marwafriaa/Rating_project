package ratingCalculation

import utils.Util
import idGeneration.IdGeneration
import ratingPreparation.RatingPreparation

object RatingCalculation {

  def main(args: Array[String]): Unit = {
    var util = new Util()
    var idg = new IdGeneration
    var ratingprep = new RatingPreparation

    val cleaned_id_df = idg.generate_new_id_integers(util,args(0))

    // Calculating final rating
    val final_res = ratingprep.calculate_rating(util,cleaned_id_df)
    final_res.show()





  }


}
