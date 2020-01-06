package com.me.spark.twitter.spark.preprocessing

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{HashingTF, IDF, StopWordsRemover, Tokenizer}

/**
  * This class preprocessed our tweet. Following are the preprocessing steps.
  * 1. Tokenize
  * 2. Remove the twitter handles in the tweet that starts with @
  * 3. Remove all the special characters
  * 4. Remove short words like ohhhhhh, yesssssss etc
  * 5. perform stemming/lemmatization
  * 6. remove stop words
  * 7. create the TFIDF feature vectors
  */
object TextPreprocessingPipeline {

  var pipeline: Pipeline = _

  def createPipeline = {
    if (pipeline == null) {
      val textTransformer = new TextTransformer()
        .setInputCol("tweet")
        .setOutputCol("preprocessedTweet")

      val tokenizer = new Tokenizer()
        .setInputCol(textTransformer.getOutputCol)
        .setOutputCol("words")

      val remover = new StopWordsRemover()
        .setInputCol(tokenizer.getOutputCol)
        .setOutputCol("filteredWords")

      val hashingTF = new HashingTF()
      hashingTF.setInputCol(remover.getOutputCol)
      hashingTF.setOutputCol("tempFeatures")

      val idf = new IDF()
      idf.setInputCol(hashingTF.getOutputCol)
      idf.setOutputCol("features")

      pipeline = new Pipeline()
      pipeline.setStages(Array(textTransformer, tokenizer, remover, hashingTF, idf))
    }
    pipeline
  }
}
