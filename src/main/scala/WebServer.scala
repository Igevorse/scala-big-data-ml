import org.scalatra._
import scala.collection.mutable.HashMap
import org.json4s._
import org.scalatra.json._
import org.json4s.jackson.Serialization.write
import scala.io.Source
case class Message(id: String, text: String)


class WebServer extends ScalatraServlet with MethodOverride{
    before() {
        
    }
    
    get("/") {
        contentType="text/html"
        Source.fromFile("frontend/index.html").mkString
    }
  
    get("/latest/?") {
        write(JettyLauncher.myML.processed_tweets)
    }
  
    // Get n tweets from the database
    get("/database/:n/?") {
        contentType="text/html"
        
        val n = params("n")
        val keys = JettyLauncher.myML.processed_tweets.keySet.toList.sorted.take(n.toInt)
        
        var data = write(JettyLauncher.myML.processed_tweets.filterKeys(keys.toSet))
        //var data = write(JettyLauncher.myML.processed_tweets)
        data = data.replaceAll("[\n]", " ")
        data = data.replaceAll("'", "\'")
        
        var html = Source.fromFile("frontend/database.html").mkString
        html = html.replace("HERE_SHOULD_BE_N", n);
        html = html.replace("JSON_DATA_HERE", data)
        html
    }
    notFound {
        "Sorry"
    }
    
    protected implicit val jsonFormats: Formats = DefaultFormats
}
