

import scala.util.matching.Regex
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.io.compress.GzipCodec

object LinkGraph 
{
   def main(args: Array[String]) 
  {
    val sparkConf = new SparkConf().setAppName("LinkGraph")
    val sc = new SparkContext(sparkConf)
    val input = sc.textFile("/Users/simeonmeerson/Desktop/small-articles-tab") // your output directory from the last assignment
      //load old text file

    val page = input.map{ l =>
      val pair = l.split("\t", 2)
        //split by tab
      (pair(0), pair(1)) // get the two fields: title and text
    }
 
    val links = page.map(r => (r._1, extractLinks(r._2))) // extract links from text
      //map out the title, call function to extract links from text

    val linkcounts = links.map(r => (r._1, r._2.split("\t").length)) // count number of links
      // save the links and the counts in compressed format (save your disk space)
 

    links.map(r => r._1 + "\t" + r._2).saveAsTextFile("./links", classOf[GzipCodec])  
    linkcounts.saveAsTextFile("./links-counts", classOf[GzipCodec])
  }
 
  def extractLinks(text: String) : String = 
  {
    val pattern = """\[\[[^:]*?\]\]""".r
      //regex patter to extract all text between [[]] except those containing ":"
    val str = text
      
    val data = (pattern findAllIn str)
      //find all regex patterns in the data

    var result = ""
    data.foreach(x => 
    {
      var rightside = x
      if(x.contains("#")){rightside=x.split("#")(0) + ("]]")}   
      if(x.contains("|")){rightside=x.split("|")(0) + ("]]")}
      if(x.contains(",")){rightside=x.split(",")(0) + ("]]")}
        //for all the lines that contain the characters
      result += (rightside+ "\t");
    })

    //val line  = data.foreach(x => {data + (x)})
  
    return result


  }
}
