import scala.io.Source                                                  //      \
import java.io.PrintWriter                                              //       \      GIVEN! import scala source
import java.io.File                                                     //       /             import java printWriter & file
import scala.collection.mutable.StringBuilder                           //      /              import stringBuilder for scala

import scala.util.matching.Regex                                        //      Regular Expression library


import org.apache.spark.SparkConf                                       //      \
import org.apache.spark.SparkContext                                    //      Spark Context
import org.apache.spark.SparkContext._                                  //      /



object PreProc {                                                                // START OF OBJECT
        def main(args: Array[String]) {                                         // START OF MAIN DRIVER PROGRAM


                //val conf = new SparkConf().setMaster("spark://Simeons-MacBook-Pro.local:7077").setAppName("PreProc")
                //val sc = new SparkContext(conf)

val inputfile = ("/Users/simeonmeerson/Documents/Academics/FALL_2015/BigDataAnalytics/Assignments/Homeworks/HW_5/wikidumps/enwiki-latest-pages-articles-multistream.xml")
            //val inputfile = ("/Users/simeonmeerson/Documents/Academics/FALL_2015/BigDataAnalytics/Assignments/Homeworks/HW_5/wikidumps/wikidump.small")
                // WIKIDUMP FILE

                val outputFile = new PrintWriter(new File("/Users/simeonmeerson/Desktop/wikiOut.txt"))
                var a_output_line = new StringBuilder


                println("----------------------------------------------------------------------------------------")
                println("Printing content from Input File")
                println("       ")                      // spaces
                println("       ")                      // spaces
                println("----------------------------------------------------------------------------------------")


            // write your code to extract content in every <page> .... </page>
            // write each of that into one line in your output file

                var counter = 0
                var printsFine = false
                var isPage = true
                //var inputline
 //        if(counter <= 20)
 //           {


        for (inputline <- Source.fromFile(inputfile).getLines)
        {        // START OF FOR LOOP
        //      val firstLine = (\\z["xml:lang="en">]").r           // RDD was to check for pages that DONt start with <page> tag
        //      val firstLine = ("xml:lang="en">").r                // RDD was to check for pages that DONT start with <page> tag
                
                val pageTag = ("<page.*?>").r                       // We'll use this regEx to identify opening page tag
        	val newLineInputStart = ("<page.*?>(.*)").r         // We'll use this regEx to identify after   page tag
                val endNewLineInput = ("</page>").r                 // We'll use this regEx to identify closing page tag
	        var foundPageStart = pageTag.findFirstIn(inputline) // Finds pageTag in Input Line

        if(foundPageStart != None)                          // If its there (not None)
                {
                    // now our RDD finds open page tag (<page.*?>) and content after (.*)
                    foundPageStart = newLineInputStart.findFirstIn(inputline)   
                }

                val foundPageEnd   = endNewLineInput.findFirstIn(inputline) // RDD to findFirst newLine END    in each input line (file)
        
                // RDD - replaced All "more than one" space with one space
                val newInputLine = inputline.replaceAll("\\s{2,}", " ")     
                //  println("new line:" + newInputLine)      PRINT statement to monitor control structure
                                
                // 2 IF STATEMENT UNDER CONDITION THAT LINE MEETS PAGE TAG
                if (foundPageStart == None)                     
                {
                    printsFine = false
                }

        else
        {  
                   //   println ("foundPageStart is <PAGE> == TRUE ")
                    printsFine = true       
        }

                // 3 if printsFine is true, then we are between opening and closing <PAGE> Tag
                // we want to print the content
        if (printsFine == true)
        {
           		 isPage = true                           
        }

        // 4 IF statement once line hits closing page tag
        if (foundPageEnd != None)
        { 
            // appends given string to our output line
            a_output_line.append("</page>\n")           

                    // prints  appended output Line (written) to a String in output File
                    outputFile.print(a_output_line.toString())      
                    
                    //  println("new line:" + a_output_line.toString())     
                           
                    // clear the output line for next parse to write into it
                    a_output_line.clear()           
                    printsFine = false
                    isPage = false
                    counter = counter + 1
		    println("  ")
                    //  outputFile.flush();
        }

        // 5 
        if (isPage == true)
        {
            // if our page is true, the append the New input line to our Output line
            a_output_line.append(newInputLine)          
        }


//     }                                           // END OF COUNTER FOR LOOP

                    print("Printing Line Number : " + counter + " ")
		    
		    println("Total Number of Pages/Files/Liness written to output")
		  //  print(counter)
                    
//       		 }                                                              // END OF FOR LOOP


      //  else{        
      //              //println("Total Number of PAGE/FILE/LINE's written to outPut")
      //              //print(counter)
      //              println("Closing File, wrote first 20 lines")
      //              println("	")	
      //    outputFile.close()
			
            }

		outputFile.close()

    }                                                                       // END OF DRIVER PROGRAM/ MAIN
}                                                                               // END OF OBJECT FUNC




