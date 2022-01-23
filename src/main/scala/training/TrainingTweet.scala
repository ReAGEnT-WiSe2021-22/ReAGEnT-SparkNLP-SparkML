package training

import java.sql.Date

/**
 * Model of tweet that will be used for Training
 * @param party party that was mentioned in the tweet
 * @param date date when tweet was created at
 * @param sentiment sentiment value of the tweet text
 *
 * @author Schander 572893
 */
case class TrainingTweet(party:String, date:Date, sentiment:Double) {

  override def toString:String = {
    "party: " + party + sys.props("line.separator") +
    "date: " + date.toString + sys.props("line.separator") +
    "sentiment: " + sentiment + sys.props("line.separator")
  }
}
