package nlp

import com.johnsnowlabs.nlp.annotator.{SentenceDetector, Tokenizer}
import com.johnsnowlabs.nlp.annotators.{LemmatizerModel, Normalizer, StopWordsCleaner}
import com.johnsnowlabs.nlp.base.{DocumentAssembler, Finisher}
import org.apache.spark.ml.Pipeline
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoders}

import utils.IOUtils
import domain.Tweet

//Preprocess the data for topic modelling
class Preprocessing(data:DataFrame) {

  private val documentAssembler = new DocumentAssembler()
    .setInputCol("text")
    .setOutputCol("document")

  private val sentenceDetector = new SentenceDetector()
    .setInputCols("document")
    .setOutputCol("sentence")

  private val tokenizer = new Tokenizer()
    .setInputCols("sentence")
    .setOutputCol("token")

  val stopwords = IOUtils.RDDFromFile("german_stopwords_short").collect
  private val stopwordCleaner = new StopWordsCleaner()
    .setInputCols("token")
    .setOutputCol("stopwordClean")
    .setStopWords(stopwords)
    .setCaseSensitive(false)

  private val normalizer = new Normalizer()
    .setInputCols("stopwordClean")
    .setOutputCol("normalized")
    //hexcode for german vowels: http://www.javascripter.net/faq/accentedcharacters.htm
    .setCleanupPatterns(Array("""[^a-zA-Z\xE4\xF6\xFC\xC4\xD6\xDC\xDF\s]""", "^http.*"))
    .setLowercase(true)

  private val lemmatizer = LemmatizerModel.load("src/main/resources/lemma_dict_ger_johnsnowlabs")
    .setInputCols("normalized")
    .setOutputCol("lemmatized")

  private val finisher = new Finisher()
    .setInputCols("lemmatized")
    .setOutputCols("cleanedText")
    .setCleanAnnotations(false)


  private val pipeline = new Pipeline().setStages(
    Array(
      documentAssembler,
      sentenceDetector,
      tokenizer,
      stopwordCleaner,
      normalizer,
      lemmatizer,
      finisher
    )
  )

  private val result = pipeline
    .fit(data)
    .transform(data)

  def getPipeline:Pipeline = pipeline

  //if verbose: gives dataframe with intermediate results of the different cleaning steps
  def getResult(verbose:Boolean = false):DataFrame = {
    if(verbose) result
    else result
      .select(
        "tweetID",
        "createdAt",
        "userID",
        "userName",
        "name",
        "party",
        "hashtags",
        "text",
        "cleanText",
        "sentiment")
  }

}
