package lda

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * User: aarokiasamy
  * Date: 4/20/16
  * Time: 5:53 AM
  */
object Validate extends App with Utils {

  if (args.length != 3) {
    println("args[0] - The original data file (e.g. StephenData500k.txt)")
    println
    println("args[1] - Validation data file (e.g. ValidationSet.csv)")
    println
    println("args[2] - Doc Distribution file (e.g. Docs-D272236-T291120-K10-I150-1461233997655).")
    println("          The one with (\"Index\", \"Topic\", \"Probability\", \"Keywords\") as a tuple")
    sys.exit()
  } else {
    println(s"Original Data file: ${args(0)}")
    println(s"Validation Data file: ${args(1)}")
    println(s"Doc Distribution file: ${args(2)}")

    setProperties()

    val sparkConf = new SparkConf().setAppName(s"Validate")
    if (System.getenv("MASTER") == null || System.getenv("MASTER").length == 0) {
      System.out.println("***** MASTER not set. Using local ******")
      sparkConf.setMaster("local[*]")
    } else {
      println("***** MASTER set to '" + System.getenv("MASTER") + "' ******")
    }
    val sc = new SparkContext(sparkConf)

    run(sc, 0, args(0), args(1), args(2), "data", "")

    sc.stop()
  }

  def run(sc: SparkContext, k: Int, dataFile: String, validationFile: String, docFile: String, topicDir: String, parent: String): (Long, Long, Long, Long, Long, Double) = {
    val sqlContext = new SQLContext(sc)

    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    val strlen = udf((str: String) => str.length)
    val parseCompetitorIds = udf((str: String) => Option(str).filter(_.trim.nonEmpty).map(_.split(";").map(_.toInt)))
    val parseKeyword3Joined = udf((str: String) => str.toLowerCase.replaceAll("[^A-Za-z0-9; ]", "_").split(";").flatMap(_.split(" ")).toSet.mkString(" ")) // toSet to unique the list
    val trim = udf((str: String) => str.trim)

    // read the original data files
    val originalData = dfZipWithIndex(sqlContext.read.parquet(dataFile).drop("Probability"))

    println
    println("Original Data. Count: " + originalData.count())
    originalData.show(5)

    // read the Doc Distribution file
    val docDistr = sc.textFile(docFile).map {
      line =>
        val split = line.substring(1, line.length - 1).split(",")
        (split(0).toInt, split(1).toInt, split(2).toDouble, split(3))
    }.toDF("Index", "Topic", "Probability", "Keywords")

    val dataWithTopics = originalData.join(docDistr).where($"Id" === $"Index")

    println
    println("Original Data with Topic assignments")
    dataWithTopics.show(5)
    val splitTopics = dataWithTopics.drop("Id").drop("Index").drop("Keywords")
    for (topic <- 0 until k) {
      splitTopics.filter($"Topic" === lit(topic)).drop("Topic").write.parquet(s"$topicDir/Topic$parent$topic")
    }

    // read the validation set
    val validationSet = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(validationFile)
      .select($"*", trim($"RelatedYesNo") as 'Related)

    validationSet.show(5)

    val compare = dataWithTopics.withColumnRenamed("CompanyId", "CompanyIdLeft").withColumnRenamed("Topic", "TopicLeft")
      .join(validationSet)
      .where($"CompanyIdLeft" === $"Company1Id")
      .join(dataWithTopics.withColumnRenamed("CompanyId", "CompanyIdRight").withColumnRenamed("Topic", "TopicRight"))
      .where($"Company2Id" === $"CompanyIdRight")

    println
    compare.show

    val compareCount = compare.count()
    val truePositives = compare.filter(($"TopicLeft" === $"TopicRight") && $"Related" === lit("Y")).count
    val trueNegatives = compare.filter(($"TopicLeft" !== $"TopicRight") && $"Related" === lit("N")).count
    val falsePositives = compare.filter(($"TopicLeft" !== $"TopicRight") && $"Related" === lit("Y")).count
    val falseNegatives = compare.filter(($"TopicLeft" === $"TopicRight") && $"Related" === lit("N")).count
    val accuracy = 1.0 * (truePositives + trueNegatives) / compareCount
    println
    println(s"Count: $compareCount")
    println(s"True Positives: $truePositives")
    println(s"True Negatives: $trueNegatives")
    println(s"False Positives: $falsePositives")
    println(s"False Negatives: $falseNegatives")
    println(s"Accuracy: $accuracy")

    (compareCount, truePositives, trueNegatives, falsePositives, falseNegatives, accuracy)
  }
}