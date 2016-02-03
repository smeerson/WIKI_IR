import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object wordCount{										//	Opening A
	def main(args:Array[String]) = {							// 	Opening B
	
		val conf = new SparkConf().setMaster("spark://Simeons-MacBook-Pro.local:7077").setAppName("wordCount")
		val sc = new SparkContext(conf)
		// val conf = new SparkConf().setMaster("local").setAppName("wordCount")


		val file_1 = sc.textFile("/Users/simeonmeerson/programming/spark_disk/spark/bin/wordCount/one.txt")
		val file_2 = sc.textFile("/Users/simeonmeerson/programming/spark_disk/spark/bin/wordCount/two.txt")




		val words1 = file_1.flatMap(I=>I.split(" "))					//	split file elements at space 	ONE.TXT
		val words2 = file_2.flatMap(I=>I.split(" "))					//	split file elements at space 	TWO.TXT


	
	val wordsLC_1 = words1.map(I=> I.toLowerCase())						// 	Words to lower Case File 1		
	val wordsLC_2 = words2.map(I=> I.toLowerCase())						//	Words to lower Case File 2

	val wordsOnly_1 = wordsLC_1.map(I=> I.replaceAll("[^A/Za-z]", " "))			//	words - non words file 1
	val wordsOnly_2 = wordsLC_2.map(I => I.replaceAll("[^A/Za-z]", " ")) 			//	words - non words file 2


	val wordsF_1 = wordsOnly_1.map(I => I.replaceAll(" ", "")).map(word=>(word, 1))		//	RDD to replace all Spaces with NOTHING f1
	val wordsF_2 = wordsOnly_2.map(I => I.replaceAll(" ", "")).map(word=>(word, 1)) 	//	RDD to replace all Spaces with NOTHING f2


	val countedWords1 = wordsF_1.count() 							//	words are counted
	//println(countedWords1)								//	print wordCount
	
	
	val countedWords2 = wordsF_2.count()							// will return int of count
	
	val RDDfile2 = wordsF_2.reduceByKey(_+_)						// RDD of words in File 2
	val file2Count = RDDfile2.count

	val RDDfile1 = wordsF_1.reduceByKey(_+_)
	val totalCount = RDDfile1.count
	
	println(totalCount, file2Count, countedWords1)
	

	// println(wordsF_1.reduceByKey(_+_).count)
	// wordsF_1.reduceByKey(_+_).collect.foreach(println)					// Words Counted println
 
	

	val unionRDD = RDDfile1.union(RDDfile2)							// value RDD to unify file1 RDD w/ file2 RDD
 	val reducedRDD = unionRDD.reduceByKey(_+_)						//

	//reducedRDD.collect.foreach(println)							// print union RDD?

	/* Extra Credit	*/

//	val args = sc.parallelize(List("Hello", "How was your day?"))
//	val string = args.mkString(" ")
//	val stringMap = string.map(x=>x.split(" "))
//	stringMap.foreach(x=>x.foreach(println))

	reducedRDD.saveAsTextFile("/Users/simeonmeerson/programming/spark_disk/projects/output1.txt")


	}											// 	Closing B
}												// 	Closing A






