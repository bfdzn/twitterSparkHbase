import java.util.Properties

import twitter4j.TwitterFactory
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

import scala.io.Source

/**
 * Clase que gestiona la conexión a Twitter
 */
case class TwitterConnection(){

  val mapKeys = getKeys
  val cb = new ConfigurationBuilder()
    .setOAuthConsumerKey(mapKeys("ConsumerKey"))
    .setOAuthConsumerSecret(mapKeys("ConsumerSecret"))
    .setOAuthAccessToken(mapKeys("AccessToken"))
    .setOAuthAccessTokenSecret(mapKeys("AccessTokenSecret")).build()

  val twitter_auth = new TwitterFactory(cb)
  val a = new OAuthAuthorization(cb)
  val atwitter: Option[twitter4j.auth.Authorization] = Some(twitter_auth.getInstance(a).getAuthorization())
  /**
   * Lee del properties los tokens de Twitter y los devuelve como un Map
   * @return Mapa clave-valor con las credenciales para conectarse a Twitter
   */
  def getKeys: Map[String, String] = {
    val url = getClass.getResource("/application.properties")
    val properties: Properties = new Properties()
    val source = Source.fromURL(url)
    properties.load(source.bufferedReader())

    Map(
      ("ConsumerKey", properties.getProperty("ConsumerKey")),
      ("ConsumerSecret", properties.getProperty("ConsumerSecret")),
      ("AccessToken", properties.getProperty("AccessToken")),
      ("AccessTokenSecret", properties.getProperty("AccessTokenSecret"))
    )
  }
}
