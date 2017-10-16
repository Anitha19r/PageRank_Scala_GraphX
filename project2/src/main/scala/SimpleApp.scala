import org.apache.spark._
import org.apache.spark.SparkContext._
import scala.xml.{XML,NodeSeq}

/**
	 * Run PageRank on XML Wikipedia dumps from http://wiki.freebase.com/wiki/WEX. Uses the "articles"
	 * files from there, which contains one line per wiki article in a tab-separated format
	 * (http://wiki.freebase.com/wiki/WEX/Documentation#articles).
	 */
	object SimpleApp         {
	  def main(args: Array[String]) {
	    if (args.length < 1) {
	      System.err.println("Usage: SparkPageRank <file> <iter>")
	      System.exit(1)
	    }

	    val sparkConf = new SparkConf().setAppName("PageRank Application")
        val inputFile = args(0)
	    val sc = new SparkContext(sparkConf)
	

	    // Parse the Wikipedia page data into a graph
      val input = sc.textFile(inputFile)
      val lines = sc.textFile(args(0), 1)

      println("Parsing input file...")
      var linkers = input.map(line => {
          val fields = line.split("\t")
          val test="\n"
          try{
             test.concat(fields(3))
          }catch{
              case e: java.lang.ArrayIndexOutOfBoundsException =>
                    System.err.println("Article body not present:\n")
                    test.concat("\n")
          }
          val (title, body) = (fields(1), test.replace("\\n", "\n"))
          val links =
          if (body == "\\N")
              NodeSeq.Empty
          else
              try {
              XML.loadString(body) \\ "link" \ "target"
              } catch {
              case e: org.xml.sax.SAXParseException =>
                  System.err.println("Article has malformed XML in body:\n")
              NodeSeq.Empty
              }
          val outEdges =  links.map(link => new String(link.text)).toArray
          (title,outEdges)
      }).cache
      println("Done parsing input file.")
      var ranks = linkers.mapValues(v => 1.0)
      val iters=10

      for (i <- 1 to iters) {
          val contribs = linkers.join(ranks).values.flatMap{ case (urls, rank) =>
          val size = urls.size
          urls.map(url => (url, rank / size))
          }
          ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
      }


      val output = ranks.collect()
      output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))
      val x = output.sortWith(_._2 > _._2)
      for( i<- 0 to 100){
           try{
             println(x(i)._1+ " has rank: " +x(i)._2)
          }catch{
              case e: java.lang.ArrayIndexOutOfBoundsException =>
                    System.err.println("Issue in fetching results\n")
          }
      }
      val final_result= x.take(100)
      val res = sc.parallelize(final_result)
      res.saveAsTextFile(args(1))
      val linesWithUniv = output.filter(line => line._1.toLowerCase.contains("university"))
      val final_univ_list = linesWithUniv.sortWith(_._2 > _._2)
      val final_univ_resut = final_univ_list.take(100)
      sc.parallelize(final_univ_resut).saveAsTextFile(args(1).concat("_Universities"))
    sc.stop()
    }
}

