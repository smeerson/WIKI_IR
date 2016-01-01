import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


import scala.util.matching.Regex

object patternExtraction{
	def main(args:Array[String]) = {
		val conf = new SparkConf().setMaster("spark://Simeons-MacBook-Pro.local:7077").setAppName("patternExtraction")
		val sc = new SparkContext(conf)

				//.setMaster("spark://Simeons-Macbook-Pro.local:7077")

		val txtOne = sc.textFile("/Users/simeonmeerson/programming/spark_disk/project/one.txt")  // RDD text One
		val txtTwo = sc.textFile("/users/simeonmeerson/programming/spark_disk/project/two.txt")	 // RDD text Two


		println("RDD containing Union (contents of both files)")
		val txtThree = txtOne.union(txtTwo)



		println("Printing Contents of Union // (both files)")
		//txtThree.collect.foreach(println)

		println("----------------------------------------------------------------------------------------")

		/*****************************************************************************************************
						PROVIDED BY SLIDES - LECTURE 6 		
						Regular Expression to extract	BIRTHDAY Dates (Born....) 
					ex:
						(born June 14, 1946)										
		*****************************************************************************************************/


		val dobRegEx = "(\\(born [^)]+\\))".r                     		      	// regular expression string = "(born ... anything up to )"
		val dob = txtThree.flatMap(line=>(dobRegEx findAllIn line))
	
		println("----------------------------------------------------------------------------------------")
	
		println("Printing DOB for each from each candidate/file")
		println("	")			// spaces
		println("	")			// spaces
		//	TEMPORARILY REMOVING FOR SCREENSHOTS		dob.collect.foreach(println)	


		println("----------------------------------------------------------------------------------------")


        /**************************************************************************************************** 		
				YEARS

        		Copied syntax from slides / example above 
        		& Regular Expressions syntax @ ---> 
         		http://www.tutorialspoint.com/scala/scala_regular_expressions.htm							
         *****************************************************************************************************/

        val years = "(\\d{4})".r									// RegEx to extract 4 consecutive digits
        val yearsExtracted 	= txtThree.flatMap(line=>(years findAllIn line)).map(word=>(word, 1))
        val yearsReduced 	= yearsExtracted.reduceByKey(_+_)
        println("Printing out Key, Value pairs of Years (4 consecutive digits)")
	// 		TEMPORARILY REMOVING FOR SCREENSHOTS	        yearsReduced.collect.foreach(println)
	println("----------------------------------------------------------------------------------------")	


	val yearsCount = yearsExtracted.count()
	println("	")
	println("	")
	println("Number of times a year (four consecutive digits) had been extracted:")
	println(yearsCount)
	println("	")
	println("	")a

	println("----------------------------------------------------------------------------------------")


        /**************************************************************************************************
				DATES:		month = (January, February, March, April, May, June, July, August, October, November, December)
						dd	= 	numbers (01-31)
						year = Four digits

				Typically appears in month, dd, year  
											
	**************************************************************************************************/



		val dates = "(([Jj]an(uary)*|[Ff]eb(ruary)*|[Mm]ar(ch)*|[Aa]pr(il)*|[Mm]ay|[Jj]un(e)*|[Jj]ul(y)*|[Aa]ug(ust)*|[Ss]ep(tember)*|[Oo]ct(ober)*|[Nn]ov(ember)*|[Dd]ec(ember)*)\\s+\\d{2}[\\s,]+\\d{4})".r
		val datesExtracted = txtThree.flatMap(line=>(dates findAllIn line))

		println("Dates Extracted from unionized RDD:")
		// TEMPORARILY COMMENTING OUT FOR SCREENSHOTS datesExtracted.collect.foreach(println)

		println("----------------------------------------------------------------------------------------")
		println("	")
		val datesCounted = datesExtracted.count()
		println("Printing total number of dates extracted: ")
		println(datesCounted)
		println("	")
		println("	")
		println("----------------------------------------------------------------------------------------")



        /***************************************************************************************************

			Quotes:
			ex:
				“ ...the agreement did not "compel the Trump organization to accept persons on welfare as 
						tenants unless as qualified as any other tenant.”	”

	***************************************************************************************************/

		println("----------------------------------------------------------------------------------------")

		val quotes = "\"([^\"]*)\"".r
		val quotesExtracted = txtThree.flatMap(line=>(quotes findAllIn line))
		println("Printing contents in Quotes for Unionized RDD")
	//		TEMPORARILY REMOVING FOR SCREENSHOTS!		quotesExtracted.collect.foreach(println)


		println("----------------------------------------------------------------------------------------")
		println("		")
		val quotesCounted = quotesExtracted.count()
		println("Total Number of times a QUOTE appears in both txt files")
		println(quotesCounted)
		println("	")
		println("----------------------------------------------------------------------------------------")



	}					// end of driver MAIN
}						// end of object pattern Extraction












