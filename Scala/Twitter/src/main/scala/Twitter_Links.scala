package twitter

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.io.{ Text, LongWritable, IntWritable }
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.twitter.TwitterUtils
import scala.io.Source
import scala.collection.mutable.ListBuffer
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import scala.util.matching.Regex
import org.apache.hadoop.fs.Path


//run it in Google Dataproc

//1- Create the structure:
//Twitter
//   |_ project
//   |    |_assembly.sbt
//   |_ src
//   |   |_ main
//   |        |_scala
//   |            |_ Twitter_Links.scala
//   |_ built.sbt
//   |_ stopwords_es.txt

//2- Run the command
//   sbt assembly

//3- Submit the job
//sudo gcloud dataproc jobs submit spark     --cluster=cluster-tests     --class twitter.Twitter_Links     --jars gs://bucket/spark/word-assembly-1.0.jar --files=stopwords_es.txt



object Twitter_Links extends App {

  def getStopWords(): ListBuffer[String] = {
    val filename = "stopwords_es.txt"
    val stopwords = ListBuffer[String]()
    for (line <- Source.fromFile(filename).getLines) {
        stopwords += StringUtils.stripAccents(line)
    }
    return stopwords
  }
 
  //Twitter properties
  System.setProperty("twitter4j.oauth.consumerKey", "")
  System.setProperty("twitter4j.oauth.consumerSecret", "")
  System.setProperty("twitter4j.oauth.accessToken", "")
  System.setProperty("twitter4j.oauth.accessTokenSecret", "")
  
  
  
  
// Get StreamingContext from checkpoint data or create a new one
  val ssc = new StreamingContext("local[*]", "TwittwerLinks", Seconds(20))
  val broadcastedStopWords = ssc.sparkContext.broadcast(getStopWords())
  val tweets = TwitterUtils.createStream(ssc, None)
  val tweetsWindows = tweets.window(Seconds(20))
  
  //get only spanish tweets
  val espanol = tweetsWindows.filter(status => status.getLang() == "es")
  //give me the tweets related with one topic
  val topic = espanol.filter(status => status.getText().contains("coronavirus"))
  
  //filtering some words from the tweet
  val rep = topic.map(status => (status.getId(), status.getText())).flatMapValues(text => text.split(" ")).filter{case (k,v) => !broadcastedStopWords.value.contains(v.toLowerCase()) && !v.trim().equals("") && v.length()>3}
  //remove blank values and marks
  val clean_val = rep.mapValues(word => word.replace("\n", "").replaceAll("""[\p{Punct}&&[^.]]""", "").toLowerCase())
  
  //calculate the cartesian values and use the format (a,b) and remove (a,a) 
  val cart = clean_val.join(clean_val).map(_._2).filter{case (a,b) => !a.equals(b)}
  //In order to avoid repeated values of the shape (b,a)
  val count = cart.map{ case(a:String,b:String)=> if (a > b) (a,b) else (b,a)}
  //Windows of 240 minutes and triggered each 20 seconds, sort the values by repeated values and save in GCS
  val save = count.countByValueAndWindow(Minutes(240), Seconds(20), 4).repartition(4).foreachRDD(RDD => RDD.sortBy(_._2, false, 4).saveAsTextFile("gs://bucket/spark/"))
  //****same behavior****
  //val save = count.map((_,1)).reduceByKeyAndWindow((a:Int,b:Int)=>a+b,Minutes(240), Seconds(20)).repartition(4)
  
  //save.foreachRDD(RDD => RDD.sortBy(_._2, false, 4).saveAsTextFile("gs://bucket/spark/"))
  
  ssc.start()
  ssc.awaitTermination()
  
}
