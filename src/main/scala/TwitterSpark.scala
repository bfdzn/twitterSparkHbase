import java.util.Properties
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j._
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import scala.io.Source

/**
 * Objeto que nos dará la conexión a Hbase
 */
object configHbase{

  val config = HBaseConfiguration.create()
  //Las propiedades de la configuración se leen de un xml
  config.addResource("./src/main/resources/hbase-site.xml")
  val connection: Connection = ConnectionFactory.createConnection(config)
  val admin = connection.getAdmin

  /**
   * Crea una tabla con una familia, es un método que también puede crear una familia en una tabla ya existente
   * @param nombreTabla es el nombre de la tabla
   * @param nombreFamilia es el nombre de la familia
   * @return No devuelve nada
   */
  def crearTabla(nombreTabla : String, nombreFamilia : String): Unit = {

    val tableName: TableName = TableName.valueOf(nombreTabla)

    if (!admin.tableExists(tableName)){
      val htableDescriptor: HTableDescriptor = new HTableDescriptor(tableName)
      htableDescriptor.addFamily(new HColumnDescriptor(nombreFamilia))
      admin.createTable(htableDescriptor)
    }else{
      for(hcolumndescriptor <- connection.getTable(tableName).getTableDescriptor.getColumnFamilies){
        if(Bytes.toString(hcolumndescriptor.getName).equals(nombreFamilia)){
          return
        }
      }
      admin.disableTable(tableName)
      admin.addColumn(tableName, new HColumnDescriptor(nombreFamilia))
      admin.enableTable(tableName)
    }
  }

  /**
   * Con este método se genera una tabla conectada a Hbase
   * @param nombreTabla es el nombre de la tabla sobre la que se va a trabajar
   * @return devuelve un objeto Table conectado a Hbase
   */
  def conectarTabla(nombreTabla : String) : Table = {
    connection.getTable(TableName.valueOf(nombreTabla))
  }

  /**
   * Cierra la conexión con Hbase
   */
  def cerrarConexion(): Unit = connection.close()

}

object TwitterSpark {

  def main(args: Array[String]): Unit = {

    //Se crea el SparkStreaming Context
    val conf = new SparkConf().setAppName("PrintTweets").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(2))
    setupLogging()

    //Se genera la configuración de Twitter, los tokens son leídos de un properties externo
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

    //Se define la familia en la que guardaremos los campos de los tweets, las columnas que tendrá dentro y el nombre de la tabla
    val familia = "texto"
    val header = Array("Texto", "Lenguaje", "Creado")
    val nombreTabla = "tweets"

    //Generamos en Hbase una tabla con la familia seleccionada. Es importante llamar a este método antes de generar el Stream
    //configHbase.crearTabla(nombreTabla,familia)

    //Se genera un Stream gracias a TwitterUtils, recoge el id, texto, lenguaje y fecha del tweet
    val tweets = TwitterUtils
      .createStream(ssc, atwitter, filters, StorageLevel.MEMORY_AND_DISK_SER)
    val statues = tweets.map(status => Array(status.getId().toString, status.getText(), status.getLang(), status.getCreatedAt().toString))

    //Por cada tweet se llama al método appendRow que guarda el tweet en Hbase
    statues.foreachRDD(rdd => rdd.foreach{x =>
      //appendRow(x.head, familia, nombreTabla, header, x.tail)
      println(x.mkString(" "))
    })
    //Empieza el stream
    ssc.start()
    //Espera la interacción del usuario
    try ssc.awaitTermination()
    catch {
      case e: Exception => println(e.toString)
    }finally configHbase.cerrarConexion()


  }

  /**
   * Ajusta el nivel de error del log4j
   */
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }

  /**
   * Agrega un tweet a la tabla, tan sólo puede agregar tweets con una familia
   *
   * @param index Es la row_key, la toma del id único de cada Tweet
   * @param family Es la familia en la que vamos a agregar
   * @param header Son los distintos campos de la familia
   * @param fields Son los valores que tomará nuestra tabla
   */
  def appendRow(index: String, family: String, tableName : String, header: Array[String], fields: Array[String]): Unit = {
    val tabla: Table = configHbase.conectarTabla(tableName)
    val row = Bytes.toBytes(index)
    val p = new Put(row)

    for (i <- 0 until fields.length) {
      p.addColumn(family.getBytes, header(i).getBytes, fields(i).getBytes)
      try tabla.put(p)
      catch {
        case e: Exception =>
          println("Error " + e.toString)
      }
    }
  }

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
