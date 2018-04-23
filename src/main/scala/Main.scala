import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{LogisticRegression, NaiveBayes}
import org.apache.spark.ml.feature.{HashingTF, IDF, RegexTokenizer, Tokenizer}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions.col
//import org.apache.spark.implicits._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.JavaConverters._
import org.apache.spark.ml.feature.StopWordsRemover
import scala.io.Source
import scala.collection.mutable.HashMap



import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ DefaultServlet, ServletContextHandler }
import org.eclipse.jetty.webapp.WebAppContext
import org.scalatra.servlet.ScalatraListener

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._

object JettyLauncher {
    var myML : MLStreaming = null
    def main(args: Array[String]) {
        val port = if(System.getProperty("http.port") != null) System.getProperty("http.port").toInt else 8080

        val server = new Server(port)
        val context = new WebAppContext()
        context.setContextPath("/")
        //context.mount(new WebServer, "/*")
        context.setResourceBase("src/main/webapp")

        context.setEventListeners(Array(new ScalatraListener))

        server.setHandler(context)

        this.myML = new MLStreaming()
        (new Thread(myML)).start()


        server.start
        server.join
    }
}


class MLStreaming extends Runnable with Serializable{
    var sparkSession : SparkSession = null
    var model : PipelineModel = null
    case class Tweet(tw_id: Long, text: String)
    case class ProcessedTweet(id:Long, text: String, pred: String)
    var tweetCount : Int = 0
    val predictions = Map(4 -> "Positive", 0 -> "Negative", 2 -> "Neutral")
    val processed_tweets = HashMap.empty[Long,ProcessedTweet]
    
    def run() {
    
        def LearnModel() : PipelineModel = {

            val columns = Seq("label", "text")
            val stopwords = Source.fromFile("stopwords.txt").getLines.toArray

            // Data: https://docs.google.com/file/d/0B04GJPshIjmPRnZManQwWEdTZjg/
            val df = this.sparkSession.read
                //          .option("header", "true")
                .option("mode", "DROPMALFORMED").csv("data/data.csv")
                .withColumnRenamed("_c0", "label")
                .withColumnRenamed("_c5", "text")


            val df_ready = df.select(columns.map(c => col(c)): _*)
            val df2 = df_ready.withColumn("label", df_ready("label").cast(IntegerType))


            val tokenizer = new Tokenizer()
                .setInputCol("text")
                .setOutputCol("words")
                
            val remover = new StopWordsRemover()
                .setInputCol("words")
                .setOutputCol("removed")
                .setStopWords(stopwords)


            val hashingTF = new HashingTF()
                .setNumFeatures(1000)
                .setInputCol(remover.getOutputCol)
                .setOutputCol("features")

            val nb = new LogisticRegression()

            val pipeline = new Pipeline()
                .setStages(Array(tokenizer, remover, hashingTF, nb))


            val model = pipeline.fit(df2)

            model.write.overwrite().save("Models/logreg")


            return model
        }
        
        try {
            val properties = new Properties()
            properties.load(new FileInputStream("src/main/properties/twitter4j.properties"))

            properties.entrySet().asScala.foreach((entry) => (sys.props += ((entry.getKey.asInstanceOf[String], entry.getValue.asInstanceOf[String]))))
        } catch {
            case e: Exception =>
                e.printStackTrace()
                sys.exit(1)
        }





        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        val sc = new SparkConf().setAppName("BravoML").setMaster("local[2]").set("spark.cassandra.connection.host", "localhost") // local
        val ssc = new StreamingContext(sc, Seconds(15))



        this.sparkSession = SparkSession.builder().appName("BravoML").getOrCreate()

        CassandraConnector(sc).withSessionDo{ session => {
                session.execute(
                        """CREATE KEYSPACE IF NOT EXISTS bdc WITH
                          | replication = { 'class': 'SimpleStrategy', 'replication_factor': 1}""".stripMargin)
                session.execute("""CREATE TABLE IF NOT EXISTS bdc.tweets (tw_id bigint, tw_text text, tw_class text, PRIMARY KEY (tw_id))""")
        }}

        
        def ProcessTweet(tweet: Tweet): Unit = {
                tweetCount+=1
                println("%d %s".format(tweetCount, tweet))
                val data = Seq(
                        Row(tweet.text)
                )

                val schema = List(StructField("text", StringType, true))

                val test = this.sparkSession.createDataFrame(
                        this.sparkSession.sparkContext.parallelize(data),
                        StructType(schema)
                )

                //model.transform(test).collect().foreach(case Row())

                val prediction = this.model.transform(test)
                  .select("prediction")
                  .collect()
//                  .foreach(m => m.values)
//                  .foreach(case (text, prediction) => )
//                  .map(m => ProcessedTweet(m.getString(0), predictions(m.getDouble(1))))

                val pred_cls = predictions(prediction(0).getDouble(0).toInt)
                println("Predicted class: " + pred_cls)
                this.processed_tweets(tweetCount) = ProcessedTweet(tweetCount, tweet.text, pred_cls)
                
                var query = """INSERT INTO bdc.tweets (tw_id, tw_text, tw_class) VALUES (""" + tweet.tw_id.toString +""", '""" + tweet.text + """', '""" + pred_cls +"""');"""
                query = query.replaceAll("'", "\'")
                println(query)
                CassandraConnector(sc).withSessionDo{ session => {
                    session.execute(query)
                    }
                }

                sparkSession.sparkContext.cassandraTable("bdc","tweets").collect().foreach(println)
        }

        try {
                this.model = PipelineModel.load("Models/logreg")
        } catch {
                case e : org.apache.hadoop.mapred.InvalidInputException => {
                        this.model = LearnModel()
                }
        }


        
        


        val stream = TwitterUtils.createStream(ssc, None)




        val twits = stream.window(Seconds(60))
          .filter((tweet) =>
                  tweet.getLang == "en"
            &&
                    (tweet.getHashtagEntities()
            .map( he => he.getText.toLowerCase())
            .contains("usa")
            ||
            tweet.getText.split(" ").map(word => word.toLowerCase).contains("trump")
            )
          )
          .map(m => Tweet(m.getCreatedAt().getTime() / 1000, m.getText)
          )

        




        twits.foreachRDD(rdd => rdd.collect().foreach(ProcessTweet))
        //twits.foreachRDD(ProcessTweets)

        ssc.start()
        ssc.awaitTermination()


        




        
    }

}
