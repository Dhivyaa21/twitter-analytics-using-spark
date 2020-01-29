package com.me.spark.twitter.preprocessing

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

class TextTransformer(override val uid: String) extends Transformer
  with HasInputCol
  with HasOutputCol
  with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("preprocessor"))

  def textPreprocessor: String => String = {
    ((tweet: String) =>
      if (tweet == null)
        "null"
      else
        removeTwitterHandles(tweet))
      .andThen(toLowerCase)
      .andThen(removeSpecialCharacters(_))
      .andThen(removeShortWords(_))
      .andThen(extractLemma(_))
  }

  /**
    * Converts the tweet to lowercase letters
    */
  val toLowerCase: String => String = {
    _.toLowerCase()
  }
  /**
    * A function that removes the twitter handles in a tweet
    */
  val removeTwitterHandles: String => String = {
    val regex = "@[\\w+]*"
    _.split("\\s+").map(_.trim.replaceAll(regex, "")).mkString(" ")
  }

  /**
    * A function that removes all the special characters including numbers
    * This just retains the letters and #
    */
  val removeSpecialCharacters: String => String = {
    val regex = "[^a-zA-Z#]"
    _.split("\\s+").map(_.trim.replaceAll(regex, "")).mkString(" ")
  }

  /**
    * Removes all the short words that are of length 3 or less
    * The intention of this function is to remove words like "ohhhhh"
    * "yessss" and slang words(function definition needs improvement).
    */
  val removeShortWords: String => String = {
    _.split("\\s+").foldLeft(List[String]())((acc, word) => {
      if (removeConsecutiveDuplicateLetters(word).trim.length < 3) acc
      else acc :+ word
    }).mkString(" ")
  }

  /**
    * Removes the consecutive duplicate characters in a word
    */
  def removeConsecutiveDuplicateLetters(word: String): String = {
    if (word == null || word.trim.length == 0) word
    else {
      word.trim.toList.tail.foldLeft(List[Char](word.toList.head))((acc, next) => {
        if (acc.last != next) acc :+ next
        else acc
      }).toString()
    }
  }

  val extractLemma: String => String = {
    Lemmatizer.lemmatize
  }

  import org.apache.spark.sql.functions.udf

  /**
    * This is the user defined function that will operate on the "tweet" column
    */
  val tweetCleaner: UserDefinedFunction = udf[String, String](textPreprocessor)

  /**
    * @inheritdoc
    */
  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)
    val metadata = outputSchema($(outputCol)).metadata
    dataset.select(col("*"), tweetCleaner(col($(inputCol))).as($(outputCol), metadata))
  }

  /**
    * @inheritdoc
    */
  override def copy(extra: ParamMap): TextTransformer = defaultCopy(extra)

  /**
    * @inheritdoc
    */
  override def transformSchema(schema: StructType): StructType = {
    val datatype = schema($(inputCol)).dataType
    schema.add(StructField("cleanedTweet", datatype, schema($(inputCol)).nullable))
  }

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)
}

object TextTransformer extends DefaultParamsReadable[TextTransformer]
