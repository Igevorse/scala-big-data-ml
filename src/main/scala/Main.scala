import java.io.FileInputStream
import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import scala.io.Source

import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.log4j.{Logger, Level}
import org.apache.spark
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{LogisticRegression, NaiveBayes}
import org.apache.spark.ml.feature.{HashingTF, IDF, RegexTokenizer, Tokenizer}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.ml.feature.StopWordsRemover

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ DefaultServlet, ServletContextHandler }
import org.eclipse.jetty.webapp.WebAppContext
import org.scalatra.servlet.ScalatraListener

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._

/********************************************************
 * This is the entry point to our Team Bravo Application.
 ********************************************************/
object BravoApplication {
    var sparkStreaming : MLStreaming = null
    def main(args: Array[String]) {
        val port = if(System.getProperty("http.port") != null) System.getProperty("http.port").toInt else 8080

        val server = new Server(port)
        val context = new WebAppContext()
        context.setContextPath("/")
        context.setResourceBase("src/main/webapp")
        context.setEventListeners(Array(new ScalatraListener))
        server.setHandler(context)

        // Run streaming as another thread to work simultaneously
        this.sparkStreaming = new MLStreaming()
        (new Thread(sparkStreaming)).start()

        // Run jetty web server
        server.start
        server.join
    }
}