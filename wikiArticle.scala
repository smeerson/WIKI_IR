import scala.util.matching.Regex
import scala.xml.XML
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object WikiArticle{		
		def main(args: Array[String])
	{
		val sparkConf = new SparkConf().setAppName("WikiArticle")		// Spark Configuration used to set parameters as key-value pairs.
		val sc = new SparkContext(sparkConf)					// SparkContext : connection to a Spark cluster; used to create RDDs,
											// Aaccumulators and Broadcast variables on cluster.
		val txt = sc.textFile("/Users/simeonmeerson/Desktop/wikiOut2.txt")	// output fromHW - Wiki Pages in lines5
		var lineCT = 0;								// Line Count
                var title = ""								// var title?
                var text = ""								// var text?
	
		val xml = txt.map{I =>						
			try{					
			    val line = XML.loadString(I)		
                            title = (line \ "title").text				// Code to get strings in <title></title>
                            text = (line \\ "text").text   			      	// Code to get strings in <text> </text>
			} catch {
				case e: Exception => println("\n\n\nException (e) case: inproper <tag> \n line num " + lineCT + I)
			}

                        lineCT = lineCT + 1						// increment lineCount
                        (title, text)							// title, and text parameters
	}	


		val stubsRegEx 		= ("-stub}}").r						// page classified as STUB
		val disambigRegEx	= """\{\{disambiguation\}\}""".r			// page classified as DISAMBIGUATION
		val redirectRegEx	= ("(#REDIRECT)").r					// page classified as REDIRECT

		var counter = 0

		def isArticle(x:String):Boolean =
		{
		   // println("I'm here \n")
		val disambigFound	= disambigRegEx.findFirstIn(x)							// RDD disambig identified
		val stubsFound		= stubsRegEx.findFirstIn(x)								// RDD stubs 	identified
		val redirectFound	= redirectRegEx.findFirstIn(x)							// RDD redirect identified

			var articleFound = false
			
			//var isArticle = false
				
			var finds = true
					
	
			if(disambigFound		 ==		None 		&&
			   stubsFound			 == 		None 		&&
			   redirectFound 		 == 		None		)
			{
				println("Article Found : " + title + " : " + lineCT )
										//  isArticle = true will not allow me to mutate boolean as val / var
				finds = false			//  Element(s) have  NOT  been located/identified in file
				return true				//  if finds false must be true (return true for file isArticle		 
			}
			else						//  Open else	(otherwise)
			{
				return false			// isArticle is false (other file classifications identified)
			}							// End Else
		}								// CLOSE isArticle function

		val articles = xml.filter {					// OPEN ARTICLES implementation
			r => isArticle(r._2.toLowerCase)			// isArticle
		}												// CLOSE articles Implementation
			
		//	val hello = articles.collect().count()
			val articlesCounted = articles.count()
		
		articles.saveAsTextFile("/users/simeonmeerson/desktop/wikiArticles2.txt")				// GENERATING COMMAS 
						// INSTEAD OF SAVE AS TEXT FILE use GIVEN save as text file line from ASSIGNMENT 8 to take care of commas \t
																										// please save - needed for later for Page Rank
		//	print(counter)
		//	print(hello)
		//	println(articlesCounted)
			println("Program Run Complete, Number of Articles found : " + articlesCounted )
 	}												// end of main
}													// end of object Wiki Article










