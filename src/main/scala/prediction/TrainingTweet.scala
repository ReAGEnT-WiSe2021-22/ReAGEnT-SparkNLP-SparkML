package prediction

import java.time.LocalDate


/**
 * Model of tweet that will be used for Training
 * @param party party that was mentioned in the tweet
 * @param text text of the tweet (unchanged)
 * @param date date when tweet was created at
 * @param sentiment sentiment value of the tweet text
 */
case class TrainingTweet(party:String, text:String, date:LocalDate, sentiment:Double) {

  override def toString:String = {
    "party: " + party + sys.props("line.separator") +
    "text: " + text + sys.props("line.separator") +
    "date: " + date.toString + sys.props("line.separator") +
    "sentiment: " + sentiment + sys.props("line.separator")
  }

}
