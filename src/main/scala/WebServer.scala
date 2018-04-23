import org.scalatra._
import scala.collection.mutable.HashMap
import org.json4s._
import org.scalatra.json._
import org.json4s.jackson.Serialization.write
import scala.io.Source

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._

class WebServer extends ScalatraServlet with MethodOverride{
    /****************************************************
     * Main page for displaying real-time 
     * processing of the stream.
     ***************************************************/
    get("/") {
        contentType="text/html"
        Source.fromFile("frontend/index.html").mkString
    }
  
    /****************************************************
     * Returns you the latest preprocessed 
     * tweets from the stream.
     ***************************************************/
    get("/latest/?") {
        write(BravoApplication.sparkStreaming.processed_tweets)
    }
  
    /****************************************************
     * Returns `n` first tweets from the database. 
     ***************************************************/
    get("/database/:n/?") {
        contentType="text/html"

        if (BravoApplication.sparkStreaming == null)
            return "Database connection is not established yet! Please try again later."

        val n = params("n")
        // Get tweets from the database
        var data = write(BravoApplication.sparkStreaming.sparkSession.sparkContext.cassandraTable("bdc","tweets").collect().take(n.toInt))
        // Prepare data to be viewed on the front-end
        data = data.replaceAll("\n", " ").replaceAll("\\n", " ").replaceAll("'", "\'").replaceAll("[\t\n\r\f]", " ")
        
        var html = Source.fromFile("frontend/database.html").mkString
        html = html.replace("HERE_SHOULD_BE_N", n);
        html = html.replace("JSON_DATA_HERE", data)
        html
    }

    notFound {
        "Sorry, this page does not exist!"
    }
    
    protected implicit val jsonFormats: Formats = DefaultFormats
}
