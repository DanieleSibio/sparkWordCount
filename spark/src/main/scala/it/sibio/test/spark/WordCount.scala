package it.sibio.test.spark

/**
 * @author Daniele Sibio
 * @Date 5/11/2015
 * In this program we will do a simple WordCount.
 */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {

  def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
    //We define a new Spark Context
    val sc = new SparkContext(conf) 

    //We create one new RDD from food.txt file that we created before.
    val test = sc.textFile("food.txt")
    //With test.flatMap we will split our words inside the food.txt file
    test.flatMap { line =>
      line.split(" ")
    }
    //Finally we map the word and we reduce them.
      .map { word => (word, 1)
      }

      .reduceByKey(_ + _)
      .saveAsTextFile("food.count.txt")

  }

}