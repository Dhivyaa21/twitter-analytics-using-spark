package com.me.spark.twitter.spark.preprocessing

import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, SentencesAnnotation, TokensAnnotation}
import edu.stanford.nlp.pipeline.StanfordCoreNLP

/**
  * Lemmatization usually refers to doing things properly with the use of a vocabulary and
  * morphological analysis of words, normally aiming to remove inflectional endings only and
  * to return the base or dictionary form of a word, which is known as the lemma.
  * Stranford NLP library provides the API to derive the lemma of words for a given sentence.
  */
object Lemmatizer {

  private var nlpPipeline: StanfordCoreNLP = _

  private def getOrCreateNlpPipeline(): StanfordCoreNLP = {
    if (nlpPipeline == null) {
      val props = new Properties()
      props.setProperty("annotators", "tokenize, ssplit, pos, lemma")
      nlpPipeline = new StanfordCoreNLP(props)
    }
    nlpPipeline
  }

  def transform(tweet: String) = {
    val pipeline = getOrCreateNlpPipeline()
    pipeline.process(tweet)
  }


  def lemmatize = (tweet: String) => {
    import scala.collection.JavaConverters._

    val annotation = transform(tweet)
    val sentences = annotation.get(classOf[SentencesAnnotation]).asScala
    val tokens = sentences.flatMap(_.get(classOf[TokensAnnotation]).asScala)
    val lemmas = tokens.map(_.get(classOf[LemmaAnnotation]))
    lemmas.toList.mkString(" ")
  }
}
