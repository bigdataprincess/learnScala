package sparkTutorial

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Level
import org.apache.log4j.Logger


object wordCount {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spc = new SparkConf().setMaster("local").setAppName("wordCounter")
    val sc = new SparkContext(spc)


    val lines = sc.textFile("/Users/bigdataprincess/Downloads/word_count.text")
    val words = lines.flatMap(line => line.split(" "))

    val wordCounts = words.countByValue()
    for ((words, count) <- wordCounts) println(words + " : " + count)
    //println(wordCounts)

  }

}
