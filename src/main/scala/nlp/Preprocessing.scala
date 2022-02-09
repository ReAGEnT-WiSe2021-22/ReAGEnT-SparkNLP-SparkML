package nlp

import com.johnsnowlabs.nlp.annotator.{SentenceDetector, Tokenizer}
import com.johnsnowlabs.nlp.annotators.{LemmatizerModel, NGramGenerator, Normalizer, StopWordsCleaner}
import com.johnsnowlabs.nlp.base.{DocumentAssembler, Finisher}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.DataFrame
import utils.IOUtils

//Preprocess the data for topic modelling
class Preprocessing(data:DataFrame) {

  //min size for tweets after preprocessing
  private val CLEAN_TEXT_MIN_SIZE = 5

  private val documentAssembler = new DocumentAssembler()
    .setInputCol("text")
    .setOutputCol("document")

  private val sentenceDetector = new SentenceDetector()
    .setInputCols("document")
    .setOutputCol("sentence")

  private val tokenizer = new Tokenizer()
    .setInputCols("sentence")
    .setOutputCol("token")

  val stopwords = IOUtils.RDDFromFile("german_stopwords_full").collect
  private val stopwordCleaner = new StopWordsCleaner()
    .setInputCols("token")
    .setOutputCol("stopwordClean")
    .setStopWords(stopwords)
    .setCaseSensitive(false)

  private val normalizer = new Normalizer()
    .setInputCols("stopwordClean")
    .setOutputCol("normalized")
    //hexcode for german vowels: http://www.javascripter.net/faq/accentedcharacters.htm
    .setCleanupPatterns(Array("^[@].*", """[^a-zA-Z\xE4\xF6\xFC\xC4\xD6\xDC\xDF\s]""", "^http.*", "^amp$"))
    .setLowercase(true)
    .setMinLength(2)

  private val lemmatizer = LemmatizerModel.load("src/main/resources/lemma_dict_ger_johnsnowlabs")
    .setInputCols("normalized")
    .setOutputCol("lemmatized")

  //build ngrams to find often reoccuring phrases for topic modeling
  private val ngramGenerator = new NGramGenerator()
    .setInputCols("lemmatized")
    .setOutputCol("ngrams")
    .setN(2)

  private val finisher = new Finisher()
    .setInputCols("ngrams")
    .setOutputCols("cleanText")
    .setCleanAnnotations(false)


  private val pipeline = new Pipeline().setStages(
    Array(
      documentAssembler,
      sentenceDetector,
      tokenizer,
      stopwordCleaner,
      normalizer,
      lemmatizer,
      ngramGenerator,
      finisher
    )
  )

  private val result = pipeline
    .fit(data)
    .transform(data)

  def getPipeline:Pipeline = pipeline

  //if verbose: gives dataframe with intermediate results of the different cleaning steps
  def getResult(verbose:Boolean = false):DataFrame = {

    val filteredResult = filterTweetsWithMinSize(result, CLEAN_TEXT_MIN_SIZE)
    if(verbose) filteredResult
    else filteredResult
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

  def filterTweetsWithMinSize(tweets:DataFrame, size:Int):DataFrame = {
    tweets.filter(row => row.getAs[List[String]]("cleanText").size >= size)
  }

  def finalize_preprocessing(tweets:DataFrame) = {
    ???
  }

}
