import java.io.FileInputStream
import java.util.Properties

import com.datastax.spark.connector.cql.CassandraConnector
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
import twitter4j.GeoLocation
//import org.apache.spark.implicits._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import com.datastax.spark.connector._

import scala.collection.JavaConverters._
import org.apache.spark.ml.feature.StopWordsRemover
import scala.io.Source



object Main extends App {

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



        val sparkSession = SparkSession.builder().appName("BravoML").getOrCreate()

        CassandraConnector(sc).withSessionDo{ session => {
                session.execute(
                        """CREATE KEYSPACE IF NOT EXISTS bdc WITH
                          | replication = { 'class': 'SimpleStrategy', 'replication_factor': 1}""".stripMargin)
                session.execute("""CREATE TABLE IF NOT EXISTS bdc.tweets (tw_id long, tw_text text, PRIMARY KEY (tw_id))""")
        }}

        var model : PipelineModel = null
        val predictions = Map(4 -> "Positive", 0 -> "Negative", 2 -> "Neutral")

        try {
                model = PipelineModel.load("Models/logreg")
        } catch {
                case e : org.apache.hadoop.mapred.InvalidInputException => {
                        model = LearnModel()
                }
        }




        val stream = TwitterUtils.createStream(ssc, None)


        case class Tweet(tw_id: Long, text: String)

        val twits = stream.window(Seconds(60))
          .filter((tweet) =>
                  tweet.getLang == "en"
            &&
//                    (tweet.getHashtagEntities()
//            .map( he => he.getText.toLowerCase())
//            .contains("usa")
//            ||
//            tweet.getText.split(" ").map(word => word.toLowerCase).contains("trump")
//            )
                  isLocationOk(tweet.getGeoLocation)
          )
          .map(m => Tweet(m.getId, m.getText)
          )

        var tweetCount : Int = 0
        //val table = sparkSession.sparkContext.cassandraTable[(Long, String)]("tw_id", "tw_text")




        twits.foreachRDD(rdd => rdd.collect().foreach(ProcessTweet))
        //twits.foreachRDD(ProcessTweets)

        ssc.start()
        ssc.awaitTermination()


        




        case class ProcessedTweet(text: String, pred: String)

        def ProcessTweet(tweet: Tweet): Unit = {
                tweetCount+=1
                println("%d %s".format(tweetCount, tweet))
                val data = Seq(
                        Row(tweet.text)
                )



                val schema = List(StructField("text", StringType, true))

                val test = sparkSession.createDataFrame(
                        sparkSession.sparkContext.parallelize(data),
                        StructType(schema)
                )

                //model.transform(test).collect().foreach(case Row())

                val prediction = model.transform(test)
                  .select("prediction")
                  //.collect()
//                  .foreach(m => m.values)
//                  .foreach(case (text, prediction) => )
//                  .map(m => ProcessedTweet(m.getString(0), predictions(m.getDouble(1))))

                // transform to rdd and put into cassandra

                prediction.rdd.saveToCassandra("bdc", "tweets")

                println("Predicted class: " + predictions(prediction.collect()(0).getDouble(0).toInt))

                sparkSession.sparkContext.cassandraTable("bdc","tweets").collect().foreach(println)
        }


        def LearnModel() : PipelineModel = {

                val columns = Seq("label", "text")

                // Data: https://docs.google.com/file/d/0B04GJPshIjmPRnZManQwWEdTZjg/
                val df = sparkSession.read
                  //          .option("header", "true")
                  .option("mode", "DROPMALFORMED").csv("data/data.csv")
                  .withColumnRenamed("_c0", "label")
                  .withColumnRenamed("_c5", "text")

                val stopwords = Source.fromFile("data/stopwords.txt").getLines.toArray



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


        def isLocationOk(geoLocation: GeoLocation) : Boolean = {
                if (geoLocation==null)
                        return false
                val lat = geoLocation.getLatitude
                val lon = geoLocation.getLongitude

                if (lat > 25 && lat<50 && lon > -130 && lon < -70)
                        return true

                return false
        }


}
