import com.mongodb.spark.MongoSpark
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.bson.Document



object TwitterSparkMongo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PrintTweets").setMaster("local[*]")
      .set("spark.mongodb.input.uri", "mongodb://127.0.0.1/twitter.tweets")
      .set("spark.mongodb.output.uri", "mongodb://127.0.0.1/twitter.tweets")
    val ssc = new StreamingContext(conf, Seconds(2))
    setupLogging()


    val twitterConnection : TwitterConnection = new TwitterConnection
    val atwitter = twitterConnection.atwitter

    //Se aplica un filtro para buscar tweets que nos interesen, en este caso será Trump
    val filters: Seq[String] = Seq("#Trump")
    val tweets = TwitterUtils
      .createStream(ssc, atwitter, filters, StorageLevel.MEMORY_AND_DISK_SER)
    val statues = tweets.map(status => Array(status.getId().toString, status.getText().replace("'","").replace("\"",""), status.getLang(), status.getCreatedAt().toString))

    statues.foreachRDD{ rdd =>
      val x = rdd.map( x => Document.parse(s"{id_tweet: '${x(0)}',texto: '${x(1)}',lang:'${x(2)}',time:'${x(3)}'}'"))
      x.foreach(x => println(x))
      MongoSpark.save(x)
    }
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * Ajusta el nivel de error del log4j
   */
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }
}
