import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming._
import org.apache.spark.SparkConf

object Main extends App {

        System.setProperty("twitter4j.oauth.consumerKey", "CONSUMER_KEY")
        System.setProperty("twitter4j.oauth.consumerSecret", "CONSUMER_SECRET")
        System.setProperty("twitter4j.oauth.accessToken", "ACCESS_TOKEN")
        System.setProperty("twitter4j.oauth.accessTokenSecret", "ACCESS_TOKEN_SECRET")

        val sc = new SparkConf().setAppName("BravoML").setMaster("local[2]") // local
        val ssc = new StreamingContext(sc, Seconds(15))
        val stream = TwitterUtils.createStream(ssc, None)


        case class Tweet(createdAt:Long, text:String)
        val twits = stream.window(Seconds(60)).map(m=>
        Tweet(m.getCreatedAt().getTime()/1000, m.toString)
        )


        twits.foreachRDD(rdd => rdd.collect().foreach(println))

        ssc.start()
        ssc.awaitTermination()
}
