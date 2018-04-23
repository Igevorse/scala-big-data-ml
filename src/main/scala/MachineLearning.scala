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

class MLStreaming extends Runnable with Serializable{
    var sparkSession : SparkSession = null
    var model : PipelineModel = null
    case class Tweet(tw_id: Long, text: String)
    case class ProcessedTweet(id:Long, text: String, pred: String)
    var tweetCount : Int = 0
    val predictions = Map(4 -> "Positive", 0 -> "Negative", 2 -> "Neutral")
    val processed_tweets = HashMap.empty[Long,ProcessedTweet]
    
    /****************************************************
    * Trains the Logistic Regression using "bag of words"
    * model and TF-IDF matrix.
    ****************************************************/
    def LearnModel() : PipelineModel = {
        val columns = Seq("label", "text")
        val stopwords = Source.fromFile("stopwords.txt").getLines.toArray

        // Data: https://docs.google.com/file/d/0B04GJPshIjmPRnZManQwWEdTZjg/
        val df = this.sparkSession.read
            .option("mode", "DROPMALFORMED").csv("data/data.csv")
            .withColumnRenamed("_c0", "label")
            .withColumnRenamed("_c5", "text")

        val df_ready = df.select(columns.map(c => col(c)): _*)
        val df2 = df_ready.withColumn("label", df_ready("label").cast(IntegerType))

        val tokenizer = new Tokenizer()
            .setInputCol("text")
            .setOutputCol("words")
            
        val stopwordsRemover = new StopWordsRemover()
            .setInputCol("words")
            .setOutputCol("removed")
            .setStopWords(stopwords)

        val hashingTF = new HashingTF()
            .setNumFeatures(1000)
            .setInputCol(stopwordsRemover.getOutputCol)
            .setOutputCol("features")

        val clf = new LogisticRegression()

        val pipeline = new Pipeline()
            .setStages(Array(tokenizer, stopwordsRemover, hashingTF, clf))

        val model = pipeline.fit(df2)

        model.write.overwrite().save("Models/logreg")
        return model
    }

    /***************************************************
     * An entry point to Spark Streaming thread.
     **************************************************/     
    def run() {
        try {
            // We store twitter API keys in properties file, which is under .gitignore
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

        // Create database if not exists
        CassandraConnector(sc).withSessionDo{ session => {
            session.execute("""CREATE KEYSPACE IF NOT EXISTS bdc WITH
                        | replication = { 'class': 'SimpleStrategy', 'replication_factor': 1}""".stripMargin)
            session.execute("""CREATE TABLE IF NOT EXISTS bdc.tweets (tw_id bigint, tw_text text, tw_class text, PRIMARY KEY (tw_id))""")
        }}

        /***************************************************
        * Predicts tweet's class and saves
        * the result to a database.
        **************************************************/   
        def ProcessTweet(tweet: Tweet): Unit = {
            tweetCount += 1
            println("%d %s".format(tweetCount, tweet))
            val data = Seq(Row(tweet.text))
            val schema = List(StructField("text", StringType, true))
            val test = this.sparkSession.createDataFrame(
                this.sparkSession.sparkContext.parallelize(data),
                StructType(schema)
            )

            val prediction = this.model.transform(test)
                .select("prediction")
                .collect()

            val pred_cls = predictions(prediction(0).getDouble(0).toInt)
            println("Predicted class: " + pred_cls)
            this.processed_tweets(tweetCount) = ProcessedTweet(tweetCount, tweet.text, pred_cls)
            
            var query = """INSERT INTO bdc.tweets (tw_id, tw_text, tw_class) VALUES (""" + tweet.tw_id.toString +""", '""" + tweet.text + """', '""" + pred_cls +"""');"""
            query = query.replaceAll("'", "\'")
            CassandraConnector(sc).withSessionDo{ session => { session.execute(query)}}
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
                tweet.getLang == "en" && 
                (tweet.getHashtagEntities()
                    .map(he => he.getText.toLowerCase())
                    .contains("usa")
                    ||
                    tweet.getText.split(" ").map(word => word.toLowerCase).contains("trump")))
            .map(m => Tweet(m.getCreatedAt().getTime() / 1000, m.getText))

        // Preprocess each tweet in the stream
        twits.foreachRDD(rdd => rdd.collect().foreach(ProcessTweet))
        ssc.start()
        ssc.awaitTermination()  
    }
}
