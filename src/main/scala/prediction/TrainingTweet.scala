package prediction

import java.sql.Date

/**
 * Model of tweet that will be used for Training
 * @param party party that was mentioned in the tweet
 * @param text text of the tweet (unchanged) (maybe the text can be removed later, since it´s not used)
 * @param date date when tweet was created at
 * @param sentiment sentiment value of the tweet text
 */
case class TrainingTweet(party:String, text:String, date:Date, sentiment:Double) {

  override def toString:String = {
    "party: " + party + sys.props("line.separator") +
    "text: " + text + sys.props("line.separator") +
    "date: " + date.toString + sys.props("line.separator") +
    "sentiment: " + sentiment + sys.props("line.separator")
  }

}
