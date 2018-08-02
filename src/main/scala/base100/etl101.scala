package base100

import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}

object etl101 {
  def main(args: Array[String]): Unit = {

    val spc = new SparkConf().setMaster("local").setAppName("baseApp")
    val sc = new SparkContext(spc)
    val sqlcontext = new org.apache.spark.sql.SQLContext(sc)

    /* Remove excess spark logs*/ sc.setLogLevel("ERROR")

    // Extract: Read file into a df
    val bh_df = sqlcontext.read.csv(path = "/Users/myname/Downloads/Boston Housing Data Set - Sheet1.csv")

    // Display the boston housing dafa frame
    //bh_df.show(3)

    // Transformation: Rename the columns properly and take out the duplicate first row
    val skip_first_row = bh_df.first() // Get the first row containing column names
    val tran_bd_df = bh_df.select(
      bh_df("_c0").alias("CRIM"),
      bh_df("_c1").alias("ZN"),
      bh_df("_c2").alias("INDUS"),
      bh_df("_c3").alias("SHAS"),
      bh_df("_c4").alias("NOX"),
      bh_df("_c5").alias("RM"),
      bh_df("_c6").alias("AGE"),
      bh_df("_c7").alias("DIS"),
      bh_df("_c8").alias("RAD"),
      bh_df("_c9").alias("TAX"),
      bh_df("_c10").alias("PT"),
      bh_df("_c11").alias("B"),
      bh_df("_c12").alias("LSTAT"),
      bh_df("_c13").alias("MV")
    ).filter(row => row != skip_first_row)

    // Display transformed df
    //tran_bd_df.show(4)

    // Load: Simple load of the transformed file
    def saveFile(tran_bd_df: DataFrame) = {
      val finalPath = "/Users/myname/Downloads/finalOutput"
      //info(s"writing to $finalPath ")
      try{
        tran_bd_df
          .write
          .mode(saveMode = "overwrite")
          .csv(s"$finalPath")
      } catch {
        case e  :Error => {
          sys.error("Houston!!!! We have a problem")
          throw e

        }
      }
    }

    saveFile(tran_bd_df)

  }
}

