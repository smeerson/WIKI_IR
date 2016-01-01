import scala.util.matching.Regex
import org.apache.spark.SparkConf 
import org.apache.spark.SparkContext 
import org.apache.spark.SparkContext._
import org.apache.hadoop.io.compress.GzipCodec



object PageRank {
	def main(args: Array[String]) {
		val sparkConf = new SparkConf().setAppName("PageRank")
		val sc = new SparkContext(sparkConf)
		// val input = sc.textFile(“./linkgraph/*.gz”) // your output directory from the last	assignment
		// val input = sc.textFile("./links/*.gz")		// output from HW 7 LinkGraph
 
//		val input = sc.textFile("/Users/simeonmeerson/Desktop/sample_outpuf_of_assignment7.txt")

		print("Loading Input -> Extracted Links (title: links")

		val input = sc.textFile("/Users/simeonmeerson/programming/spark_disk/project/links")	
		   val page = input.map{ l =>
        		    val pair = l.split("\t", 2)             			// assuming "\t" is the delimiter in the input file
        		    (pair(0), pair(1))                      			// get the two fields: title and text
    		}		





    	val ITERATION = 20
		
		val links = page.map(page =>(page._1.substring(1), extractLinks(page._2))) 
		// Load RDD of (page title, links) pairs val ranks = // Load RDD of (page title, rank) pairs
		
		var ranks = links.mapValues(v => 1.0)
		
		for (i <- 0 to ITERATION) {
			 var contributions = links.join(ranks).flatMap 
			 		{ case (pageID, (pageLinks, rank)) =>
			 			pageLinks.map(dest => (dest, rank / pageLinks.length))
			 	}

			 ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85 * v)
			 // page.first
			 // print(ranks)
		}

		



// Implement your PageRank algorithm according to the notes ...
// Sort pages by their PageRank scores ranks.sortBy ...
// save the page title and pagerank scores in compressed format (save your disk space). Using “\t” as the delimiter.



 		ranks.map(r => r._1 + "\t" + r._2).saveAsTextFile("./pageranks", classOf[GzipCodec])
	//	ranks.map(r => r._1 + "\t" + r._2).saveAsTextFile("./pageranks")
	} 

   	 def extractLinks(links:String):Array[String] = {
        	//1.split the links by the \t
        	    var list = links.split("\t")
        	//2.get rid of brackets
        	    var listWithoutBrackets = list.map{link => 
        	        if(link.length>=4) 
                	    link.substring(2,link.length-2) 
                	else 
                	    link
                	}
      		 listWithoutBrackets
    }

}



