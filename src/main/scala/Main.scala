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

        val sc = new SparkConf().setAppName("BravoML").setMaster("local[2]") // local
        val ssc = new StreamingContext(sc, Seconds(15))



        val sparkSession = SparkSession.builder().appName("BravoML").getOrCreate()

        var model : PipelineModel = null
        val predictions = Map(4 -> "Positive", 0 -> "Negative", 2 -> "Neutral")
        val stopwords = Source.fromFile("stopwords.txt").getLines.toArray

        try {
                model = PipelineModel.load("Models/logreg")
        } catch {
                case e : org.apache.hadoop.mapred.InvalidInputException => {
                        model = LearnModel()
                }
        }




        val stream = TwitterUtils.createStream(ssc, None)


        case class Tweet(createdAt: Long, text: String)

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

        var tweetCount : Int = 0




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
                  .collect()
//                  .foreach(m => m.values)
//                  .foreach(case (text, prediction) => )
//                  .map(m => ProcessedTweet(m.getString(0), predictions(m.getDouble(1))))


                println("Predicted class: " + predictions(prediction(0).getDouble(0).toInt))
        }


        def LearnModel() : PipelineModel = {

                val columns = Seq("label", "text")

                // Data: https://docs.google.com/file/d/0B04GJPshIjmPRnZManQwWEdTZjg/
                val df = sparkSession.read
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


}
