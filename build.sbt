name := "party_prediction"

version := "0.1"

scalaVersion := "2.12.14"

val sparkVersion = "3.0.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.mongodb.scala" %% "mongo-scala-driver" % "2.9.0",
  "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.1",
  "com.johnsnowlabs.nlp" %% "spark-nlp" % "3.3.1",
  //Sentiment
  "edu.stanford.nlp" % "stanford-corenlp" % "4.2.2",
  "edu.stanford.nlp" % "stanford-corenlp" % "4.2.2" classifier "models"

  //topic model evaluation => DEPENDENCY NOT WORKING
  //"io.github.gnupinguin" %% "ldacoherence_2.12" % "1.0"
)
