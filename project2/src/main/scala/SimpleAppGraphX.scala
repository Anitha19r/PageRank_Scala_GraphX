import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.xml.{XML,NodeSeq}
object SimpleAppGraphX         {
	  def main(args: Array[String]) {
	    if (args.length < 2) {
	      System.err.println("Usage: GraphXPageRank <file> <output_file>")
	      System.exit(1)
	    }
        val sparkConf = new SparkConf().setAppName("PageRank Application")
        val inputFile = args(0)
        val sc = new SparkContext(sparkConf)
        //Load the Wikipedia Articles———
        val wiki: RDD[String] = sc.textFile(args(0)).coalesce(20)
        wiki.first

        //Clean the Data—————
        // Define the article class
        case class Article(val title: String, val body: String)

        // Parse the articles
        val articles = wiki.map(_.split('\t')).
        // two filters on article format
        filter(line => (line.length > 1 && !(line(1) contains "REDIRECT"))).
        // store the results in an object for easier access
        map(line => new Article(line(1).trim, line(3).trim)).cache

        articles.count

        //Making a Vertex RDD————
        // Hash function to assign an Id to each article
        def pageHash(title: String): VertexId = {
        title.toLowerCase.replace(" ", "").hashCode.toLong
        }
        // The vertices with id and article title:
        val vertices: RDD[(VertexId, String)] = articles.map(a => (pageHash(a.title), a.title)).cache
        vertices.count

        //Making the Edge RDD ----- 
        val edges_list = articles.flatMap { a =>
        val srcVid = pageHash(a.title)
        val links =
                    if (a.body == "\\N")
                    NodeSeq.Empty
                    else
                    try {
                        XML.loadString(a.body) \\ "link" \ "target"
                    } catch {
                        case e: org.xml.sax.SAXParseException =>
                        System.err.println("Article has malformed XML in body:\n")
                        NodeSeq.Empty
                    }
            val arrayChk =links.map(link => {
                val dest = new String(link.text)
                (srcVid,pageHash(dest))
                })
            //println(arrayChk.getClass)
            arrayChk
        }.cache
        val graph = Graph.fromEdgeTuples(edges_list,1)
        val ranks = graph.pageRank(0.01).vertices
        val ranksByUsername =vertices.join(ranks)map {
        case (id, (name, rank)) => (name,rank)
        }
        val output = ranksByUsername.collect()
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
        sc.stop()
      }
}

