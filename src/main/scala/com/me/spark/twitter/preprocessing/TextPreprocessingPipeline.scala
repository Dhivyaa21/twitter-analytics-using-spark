package com.me.spark.twitter.preprocessing

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature._

/**
  * This class preprocessed our tweet. Following are the preprocessing steps.
  * 1. Tokenize
  * 2. Remove the twitter handles in the tweet that starts with @
  * 3. Remove all the special characters
  * 4. Remove short words like ohhhhhh, yesssssss etc
  * 5. perform stemming/lemmatization
  * 6. remove stop words
  * 7. create the TFIDF feature vectors
  * 8. NGrams
  */
object TextPreprocessingPipeline {

  var pipeline: Pipeline = _
  var cleanTweetPipeline: Pipeline = _
  var tfIdfPipeline: Pipeline = _
  var nGramsPipeline: Pipeline = _

  def createPipeline = {
    if (pipeline == null) {
      pipeline = new Pipeline()
      pipeline.setStages(
        Array(createCleanTweetPipeline(),
        createTfIdfPipeline(),
        createNGramPipeline()))
    }
    pipeline
  }

  def createCleanTweetPipeline() = {
    if (cleanTweetPipeline == null) {
      val textTransformer = new TextTransformer()
        .setInputCol("tweet")
        .setOutputCol("cleanedTweet")

      val tokenizer = new Tokenizer()
        .setInputCol(textTransformer.getOutputCol)
        .setOutputCol("words")

      val remover = new StopWordsRemover()
        .setInputCol(tokenizer.getOutputCol)
        .setOutputCol("filteredWords")

      cleanTweetPipeline = new Pipeline()
      cleanTweetPipeline.setStages(Array(textTransformer, tokenizer, remover))
    }
    cleanTweetPipeline
  }

  def createTfIdfPipeline() = {
    if (tfIdfPipeline == null) {
      val hashingTF = new HashingTF()
      hashingTF.setNumFeatures(20)
      hashingTF.setInputCol("filteredWords")
      hashingTF.setOutputCol("tempFeatures")

      val idf = new IDF()
      idf.setInputCol(hashingTF.getOutputCol)
      idf.setOutputCol("tfidf")

      tfIdfPipeline = new Pipeline()
      tfIdfPipeline.setStages(Array(hashingTF, idf))
    }
    tfIdfPipeline
  }

  def createNGramPipeline() = {
    if (nGramsPipeline == null) {
      val nGrams = new NGram().setInputCol("filteredWords").setOutputCol("nGrams")
      val vectors = new CountVectorizer().setInputCol(nGrams.getOutputCol).setOutputCol("ngramFeatureVector")
      val vectorAssembler = new VectorAssembler().setInputCols(Array("ngramFeatureVector", "tfidf")).setOutputCol("features")
      nGramsPipeline = new Pipeline()
      nGramsPipeline.setStages(Array(nGrams, vectors, vectorAssembler))
    }
    nGramsPipeline
  }
}
