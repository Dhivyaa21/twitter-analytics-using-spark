# twitter-analytics-using-spark
Classification of tweets as Negative, neutral or positive

Tools used
* scala 2.12.10 
* Spark-ML 3.0.0-preview2 (latest)
* Spark-twitter-Streaming 2.4.4
* Stranford-NLP library 

The application uses the [sentiment140](http://help.sentiment140.com/for-students) dataset for training

The tweets are loaded into a dataframe, preprocessed before extracting features.

Assemble the fat jar with dependencies using the following command

`sbt -J-Xms2048m -J-Xmx2048m assembly`

To get the assembly task, add ``plugins.sbt`` file in `project/` folder with the following plugin

`addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")`

The above plugin will add an assembly task to create the fat jar.

To run this app, use the following command

`spark-submit --class "com.me.spark.twitter.sparkml.Sentiment140TweetsClassifier" --master local[*] --d
 river-memory 4G --executor-cores 3 target/scala-2.12/sentiment-analysis-0.1.jar `
 
 To run real time predictions, Use the `TweetStreaming` class's main method.
 For this class to run, you need the twitter.txt file with twitter credentials. 
 To set up a developer account visit [Twitter Developer platform](https://developer.twitter.com/)
 
 * Limitations
 
   ** Naive bayes classification results do not have good accuracy.
   ** Logistic regression does better but still the prediction accuracy on test dataset is close to 80 percent.
   ** More work needs to be done in optimizing the prediction process.
 
 


