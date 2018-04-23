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
        Source.fromFile("frontend/index.html").mkString
        val n = params("n")
        val keys = JettyLauncher.myML.processed_tweets.keys().toList().sorted.take(n)
        
        write(JettyLauncher.myML.processed_tweets.filterKeys(keys.toSet)
    }
    notFound {
        "Sorry"
    }
    
    protected implicit val jsonFormats: Formats = DefaultFormats
}
