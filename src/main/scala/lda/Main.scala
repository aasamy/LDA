package lda

import java.util.Date

import org.apache.spark.examples.mllib.{AbstractParams, LDAExample}
import scopt.OptionParser

/**
  * User: aarokiasamy
  * Date: 4/24/16
  * Time: 8:56 PM
  */
object Main extends App with Utils {

  case class Params(dataFile: String = "", k: Int = 20, fromK: Int = 20, toK: Int = 20, stepK: Int = 10,
                    maxIterations: Int = 10, docConcentration: Double = -1, topicConcentration: Double = -1,
                    vocabSize: Int = 10000, stopwordFile: String = "", algorithm: String = "em",
                    checkpointDir: Option[String] = None, checkpointInterval: Int = 10, numPartitions: Int = 8,
                    outputDir: String = "data", validationFile: String = "") extends AbstractParams[Params] {
    val name = s"K$k-I$maxIterations-TS${new Date().getTime}"
    val keywordsFile = s"$outputDir/Keywords-$name"
    val topicDir = s"$outputDir/Topic-K$k-I$maxIterations"
  }

  setProperties()

  val defaultParams = Params()

  val parser = new OptionParser[Params]("LDAExample") {
    head("LDAExample: an example LDA app for plain text data.")
    opt[Int]("k")
      .text(s"number of topics. default: ${defaultParams.k}")
      .action((x, c) => c.copy(k = x))
    opt[Int]("fromK")
      .text(s"Starting parameter sweep for k. default: ${defaultParams.fromK}")
      .action((x, c) => c.copy(fromK = x))
    opt[Int]("toK")
      .text(s"Ending parameter sweep for k. default: ${defaultParams.toK}")
      .action((x, c) => c.copy(toK = x))
    opt[Int]("stepK")
      .text(s"Ending parameter sweep for k. default: ${defaultParams.stepK}")
      .action((x, c) => c.copy(stepK = x))
    opt[Int]("maxIterations")
      .text(s"number of iterations of learning. default: ${defaultParams.maxIterations}")
      .action((x, c) => c.copy(maxIterations = x))
    opt[Double]("docConcentration")
      .text(s"amount of topic smoothing to use (> 1.0) (-1=auto). default: ${defaultParams.docConcentration}")
      .action((x, c) => c.copy(docConcentration = x))
    opt[Double]("topicConcentration")
      .text(s"amount of term (word) smoothing to use (> 1.0) (-1=auto). default: ${defaultParams.topicConcentration}")
      .action((x, c) => c.copy(topicConcentration = x))
    opt[Int]("vocabSize")
      .text(s"number of distinct word types to use, chosen by frequency (-1=all). default: ${defaultParams.vocabSize}")
      .action((x, c) => c.copy(vocabSize = x))
    opt[String]("stopwordFile")
      .text(s"filepath for a list of stopwords. Note: This must fit on a single machine." +
        s"  default: ${defaultParams.stopwordFile}")
      .action((x, c) => c.copy(stopwordFile = x))
    opt[String]("algorithm")
      .text(s"inference algorithm to use. em and online are supported. default: ${defaultParams.algorithm}")
      .action((x, c) => c.copy(algorithm = x))
    opt[String]("checkpointDir")
      .text(s"Directory for checkpointing intermediate results. Checkpointing helps with recovery and eliminates " +
        s"temporary shuffle files on disk. default: ${defaultParams.checkpointDir}")
      .action((x, c) => c.copy(checkpointDir = Some(x)))
    opt[Int]("checkpointInterval")
      .text(s"Iterations between each checkpoint.  Only used if checkpointDir is set." +
        s" default: ${defaultParams.checkpointInterval}")
      .action((x, c) => c.copy(checkpointInterval = x))
    opt[String]("outputDir")
      .text(s"Directory for output results. Location to write the LDA Model and Document assignments." +
        s"  default: ${defaultParams.outputDir}")
      .action((x, c) => c.copy(outputDir = x))
    opt[String]("validationFile")
      .text(s"Validation file with possitive and negative labels." +
        s"  default: ${defaultParams.validationFile}")
      .action((x, c) => c.copy(validationFile = x))
    opt[Int]("numPartitions")
      .text(s"number of partitions to repartition the input data. default: ${defaultParams.numPartitions}")
      .action((x, c) => c.copy(numPartitions = x))
    arg[String]("<dataFile>")
      .text("dataFile")
      .required()
      .action((x, c) => c.copy(dataFile = x))
  }

  parser.parse(args, defaultParams).foreach {
    params => doRun(params, "-", 4) // 5 levels
  }

  def doRun(params: Params, parent: String, level: Int): Unit = {
    DataPrep.run(params.dataFile, params.keywordsFile)
    val k = params.k
    val (logLikelihood, median, docFile) = LDAExample.run(params)

    Option(params.validationFile).filter(_.nonEmpty).foreach {
      s => val (compareCount, truePositives, trueNegatives, falsePositives, falseNegatives, accuracy) =
        Validate.run(k, params.dataFile, params.validationFile, docFile, params.topicDir, parent)

        if (level > 0) {
          for (t <- 0 until k) {
            doRun(params.copy(dataFile = s"${params.topicDir}/Topic$parent$t"), s"$parent$t-", level-1)
          }
        }
    }
  }
}