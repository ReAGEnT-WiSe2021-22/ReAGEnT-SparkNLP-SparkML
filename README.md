# ReAGEnT-SparkNLP-SparkML

# Party Reputation:

With the help of the Spark ml library all tweets extracted with the Twint API will be analyzed.
The application uses for sentiment detection the StandfordCoreNLP library
The Training uses Linear Regression to train a model that can predict the reputation of a party for a specific day.

The model, as well as the real sentiment values for each day, will be saved to the MongoDB after training.


The model contains: party: String, dates: List[String], sentiments: List[Double]

##Prerequisites:

- The URI for the DB has to be defined in the system environment variables as `REAGENT_MONGO`
- Tweets will be loaded from the collection: `political_tweets_2021`
- The outcome will be written to the collections `ml_party_reputation_predictions` & `ml_party_reputation_labels`
  
- Run the program with `sbt run`

-------------------
# Potential Party:
