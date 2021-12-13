package prediction

import java.time.LocalDateTime

/**
 * Model of tweet that will be used for Training
 * @param party party that was mentioned in the tweet
 * @param text text of the tweet (unchanged)
 * @param date date when tweet was created at
 * @param Sentiment sentiment value of the tweet text
 */
class TrainingTweet(party:String, text:String, date:LocalDateTime, Sentiment:Double) {

}
