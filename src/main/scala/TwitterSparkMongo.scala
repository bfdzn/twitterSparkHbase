import TwitterSpark.getKeys
import com.mongodb.spark.MongoSpark
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.TwitterFactory
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import org.bson.Document
import com.mongodb.spark.config._



object TwitterSparkMongo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PrintTweets").setMaster("local[*]")
      .set("spark.mongodb.input.uri", "mongodb://127.0.0.1/twitter.tweets")
      .set("spark.mongodb.output.uri", "mongodb://127.0.0.1/twitter.tweets")
    val ssc = new StreamingContext(conf, Seconds(2))
    val mapKeys = getKeys
    val cb = new ConfigurationBuilder()
      .setOAuthConsumerKey(mapKeys("ConsumerKey"))
      .setOAuthConsumerSecret(mapKeys("ConsumerSecret"))
      .setOAuthAccessToken(mapKeys("AccessToken"))
      .setOAuthAccessTokenSecret(mapKeys("AccessTokenSecret")).build()

    val twitter_auth = new TwitterFactory(cb)
    val a = new OAuthAuthorization(cb)
    val atwitter: Option[twitter4j.auth.Authorization] = Some(twitter_auth.getInstance(a).getAuthorization())

    //Se aplica un filtro para buscar tweets que nos interesen, en este caso será Coronavirus
    val filters: Seq[String] = Seq("#Trump")
    val tweets = TwitterUtils
      .createStream(ssc, atwitter, filters, StorageLevel.MEMORY_AND_DISK_SER)
    val statues = tweets.map(status => Array(status.getId().toString, status.getText(), status.getLang(), status.getCreatedAt().toString))

    statues.foreachRDD{ rdd =>
      val x = rdd.map(x => Document.parse(s"{spark: 'hola'}"))
      MongoSpark.save(x)
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
