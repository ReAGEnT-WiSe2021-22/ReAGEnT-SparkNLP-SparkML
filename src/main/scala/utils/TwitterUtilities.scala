package utils

import java.time.{Instant, LocalDateTime, OffsetDateTime}
import java.time.format.DateTimeFormatter
import java.util.Locale
import java.sql.Timestamp
import prediction.TrainingTweet


object TwitterUtilities {

  /**
    * Parses a given tweet in the Twitter Data JSON Format using [[JSONUtils.parseJson()]]
    * and extracts the date, username, text, party and hashtags of the tweet into a [[TrainingTweet]] object.
    * If the line is not a valid json string, None is returned instead.
    *

  The easiest way to parse a tweet is to convert it to a Map and extract the components by using the keys
  Missing values or wrong formats could be handled by setting default values

  Model:
  case class TrainingTweet(partei:String, text:String, date:LocalDateTime, sentiment:Double)
   */

  def parse(jsonString: String): Option[TrainingTweet] = {
    val tweet = JSONUtils.parseJson(jsonString) //Option[ Map[String, Any] ]

    tweet match {
      case Some(x) => val map = x
        map.get("id") match {
          case Some(_) => {

            val date = map.get("created_at") match {
              case Some(txt:String) => this.transformTwitterDate(txt)
              case _ => null
            }

            val text = map.get("tweet") match {
              case Some(txt:String) => txt
              case _ => ""
            }

            val partei = map.get("partei") match {
              case Some(txt:String) => txt
              case _ => "Parteilos"
            }

            val tweet_ = new TrainingTweet(partei, text, date, SentimentAnalysisUtils.detectSentiment(text))

            Option(tweet_)
          }
          case _ => None
        }

      case None => None
    }
  }
  /*

  Example Tweet:
  {"id":1374807339895775232,"conversation_id":"1374797990540554245","created_at":"2021-03-24 20:36:27 CET",
  "date":"2021-03-24","time":"20:36:27","timezone":"+0200","user_id":378693834,"username":"peteraltmaier",
  "name":"Peter Altmaier","place":"","tweet":"@mrdarcysblog Ich teile Ihre Einschätzung zur Gastronomie &amp;
  weiß um die Not. Deshalb Hilfen in Milliardenhöhe (jetzt noch verbessert). Und wir wollten für Tische im Freien
   ab 22.3. öffnen. Der exponentielle Anstieg hat das verhindert. Wenn wir Lösung wollen, müssen wir anderswo mehr
    machen.","language":"de","mentions":[],"urls":[],"photos":[],"replies_count":1,"retweets_count":1,
    "likes_count":2,"hashtags":[],"cashtags":[],"link":"https://twitter.com/peteraltmaier/status/1374807339895775232",
    "retweet":false,"quote_url":"","video":0,"thumbnail":"","near":"","geo":"","source":"","user_rt_id":"",
    "user_rt":"","retweet_id":"","reply_to":[{"screen_name":"mrdarcysblog","name":"misterdarcysblog","id":"3344427652"}],
    "retweet_date":"","translate":"","trans_src":"","trans_dest":"","partei":"CDU"}
   */

  /*
    Helper Method for specific Data Types
   */
  def getNumber(map:Map[String,Any], key:String):Option[Long]= map.get(key) match {

    case Some(i:Int) => Some(i.toLong)
    case Some(l:Long) => Some(l)
    case _ => None
  }

  val dtf: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  /**
   * Transforms a string-timestamp from Twitter into a LocalDateTime object
   * @param date Date field of tweet
   * @return LocalDateTime object
   */
  def transformTwitterDate(date:String):LocalDateTime = LocalDateTime.parse(date.take(19), dtf)
}