
package WordCount

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object Test {
  
  def main(args: Array[String] )= {
   

System.setProperty("hadoop.home.dir", "/Users/saifniazi/workspace/Hadoop/");
// create Spark context with Spark configuration
val sc = new SparkContext(new SparkConf()
.setAppName("Spark WordCount")
.setMaster("local[2]"))

//Load inputFile
val inputFile = sc.textFile("resources/input.txt")
val counts = inputFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
counts.foreach(println)

sc.stop()
  }
 
} 
