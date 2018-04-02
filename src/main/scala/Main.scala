import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.collection.JavaConverters._

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


        twits.foreachRDD(rdd => rdd.collect().foreach(ProcessTweet))

        ssc.start()
        ssc.awaitTermination()

        var tweetCount : Int = 0


        def ProcessTweet(tweet: Tweet): Unit = {
                tweetCount+=1
                println("%d %s".format(tweetCount, tweet))
        }
}
