/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples.mllib

import java.text.BreakIterator
import java.util.Date

import lda.Main.Params

import scala.collection.mutable

import scopt.OptionParser

import org.apache.log4j.{Level, Logger}

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.clustering.{EMLDAOptimizer, OnlineLDAOptimizer, DistributedLDAModel, LDA}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD


/**
  * An example Latent Dirichlet Allocation (LDA) app. Run with
  * {{{
  * ./bin/run-example mllib.LDAExample [options] <input>
  * }}}
  * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
  */
object LDAExample {

  def run(params: Params): (Double, Double, String) = {
    val sparkConf = new SparkConf().setAppName(s"LDAExample with $params")
    if (System.getenv("MASTER") == null || System.getenv("MASTER").length == 0) {
      System.out.println("***** MASTER not set. Using local ******")
      sparkConf.setMaster("local[*]")
    } else {
      println("***** MASTER set to '" + System.getenv("MASTER") + "' ******")
    }
    val sc = new SparkContext(sparkConf)

    Logger.getRootLogger.setLevel(Level.WARN)

    // Load documents, and prepare them for LDA.
    val preprocessStart = System.nanoTime()
    val (corpus, vocabArray, actualNumTokens, textIdRDD) = preprocess(sc, params.keywordsFile, params.vocabSize, params.stopwordFile, params.numPartitions)
    corpus.cache()
    val actualCorpusSize = corpus.count()
    val actualVocabSize = vocabArray.size
    val preprocessElapsed = (System.nanoTime() - preprocessStart) / 1e9

    println()
    println(s"Corpus summary:")
    println(s"\t Training set size: $actualCorpusSize documents")
    println(s"\t Vocabulary size: $actualVocabSize terms")
    println(s"\t Training set size: $actualNumTokens tokens")
    println(s"\t Preprocessing time: $preprocessElapsed sec")
    println(s"\t Max Iterations: ${params.maxIterations}")
    println()

    val outputName = s"D$actualCorpusSize-T$actualVocabSize-${params.name}"

    // Run LDA.
    val lda = new LDA()

    val optimizer = params.algorithm.toLowerCase match {
      case "em" => new EMLDAOptimizer
      // add (1.0 / actualCorpusSize) to MiniBatchFraction be more robust on tiny datasets.
      case "online" => new OnlineLDAOptimizer().setMiniBatchFraction(0.05 + 1.0 / actualCorpusSize)
      case _ => throw new IllegalArgumentException(
        s"Only em, online are supported but got ${params.algorithm}.")
    }

    lda.setOptimizer(optimizer)
      .setK(params.k)
      .setMaxIterations(params.maxIterations)
      .setDocConcentration(params.docConcentration)
      .setTopicConcentration(params.topicConcentration)
      .setCheckpointInterval(params.checkpointInterval)
    if (params.checkpointDir.nonEmpty) {
      sc.setCheckpointDir(params.checkpointDir.get)
    }
    val startTime = System.nanoTime()
    val ldaModel = lda.run(corpus)
    val elapsed = (System.nanoTime() - startTime) / 1e9

    println(s"Finished training LDA model.  Summary:")
    println(s"\t Training time: $elapsed sec")

    val (logLikelihood, median, docFile) = ldaModel match {
      case distLDAModel: DistributedLDAModel =>
        val avgLogLikelihood = distLDAModel.logLikelihood / actualCorpusSize.toDouble
        println(s"\t Training data average log likelihood: $avgLogLikelihood")
        println()

        // Print the topics, showing the top-weighted terms for each topic.
        val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
        val topics = topicIndices.map { case (terms, termWeights) =>
          terms.zip(termWeights).map { case (term, weight) => (vocabArray(term.toInt), weight) }
        }

//        // also print top 5 documents in each topic
//        val docsForTopic = distLDAModel.topDocumentsPerTopic(5)

        println(s"${params.k} topics:")
        topics.zipWithIndex.foreach { case (topic, i) =>
          println(s"TOPIC $i")
          topic.foreach { case (term, weight) =>
            println(s"$term\t$weight")
          }
          println()

//          // top documents in topic
//          val (docIds, weights) = docsForTopic(i)
//          for (ii <- docIds.indices) {
//            println(s"${docIds(ii)}\t${weights(ii)}")
//          }
//          println()
//          println()
        }

        val topicDist = distLDAModel.topicDistributions.map {
          case (documentId, topicDistribution) =>
            // its a topic distribution, cannot assume head is the one
            val (probability, topic) = topicDistribution.toArray.zipWithIndex.sortBy(x => -x._1).head
            (documentId, (topic, probability))
        }

        val docs = topicDist.join(textIdRDD.map(x => (x._2, x._1))).map(x => (x._1, x._2._1._1, x._2._1._2, x._2._2))

        docs.take(10).foreach {
          case (documentId, topic, probability, keywords) =>
            println("Document: " + documentId + " Topic: " + topic + "(" + probability + ") - " + keywords)
        }

        val docProbabilites = docs.map(_._3)
        val (buckets, counts) = docProbabilites.histogram(10)
        println()
        for (ii <- counts.indices) {
          println(s"$ii : ${buckets(ii)} : ${counts(ii)}")
        }
        println(s"${buckets.length - 1} : ${buckets.last}")

        val m = getMedian(docProbabilites)
        println()
        println(s"Median: $m ${docProbabilites.stats()}")
        println()

        println("Saving Docs...")
        val docFile = s"${params.outputDir}/Docs-$outputName-M$m"
        docs.sortBy(x => x._1, numPartitions = 1).saveAsTextFile(docFile)

        (avgLogLikelihood, m, docFile)
      case _ =>
        (0.0, 0.0, "")
    }

    println("Saving Model...")
    ldaModel.save(sc, s"${params.outputDir}/LDAModel-$outputName")

    sc.stop()

    (logLikelihood, median, docFile)
  }

  /**
    * Load documents, tokenize them, create vocabulary, and prepare documents as term count vectors.
    * @return (corpus, vocabulary as array, total token count in corpus)
    */
  private def preprocess(sc: SparkContext,
                         path: String,
                         vocabSize: Int,
                         stopwordFile: String,
                         numPartitions: Int): (RDD[(Long, Vector)], Array[String], Long, RDD[(String, Long)]) = {

    // Get dataset of document texts
    // One document per line in each text file. If the input consists of many small files,
    // this can result in a large number of small partitions, which can degrade performance.
    // In this case, consider using coalesce() to create fewer, larger partitions.
    val textRDD: RDD[String] = sc.textFile(path)
    val textIdRDD = textRDD.zipWithIndex()

    // Split text into words
    val tokenizer = new SimpleTokenizer(sc, stopwordFile)
    val tokenized: RDD[(Long, IndexedSeq[String])] = textIdRDD.map { case (text, id) =>
      id -> tokenizer.getWords(text)
    }
    tokenized.cache()

    // Counts words: RDD[(word, wordCount)x]
    val wordCounts: RDD[(String, Long)] = tokenized
      .flatMap { case (_, tokens) => tokens.map(_ -> 1L) }
      .reduceByKey(_ + _)
    wordCounts.cache()
    val fullVocabSize = wordCounts.count()
    // Select vocab
    //  (vocab: Map[word -> id], total tokens after selecting vocab)
    val (vocab: Map[String, Int], selectedTokenCount: Long) = {
      val tmpSortedWC: Array[(String, Long)] = if (vocabSize == -1 || fullVocabSize <= vocabSize) {
        // Use all terms
        wordCounts.collect().sortBy(-_._2)
      } else {
        // Sort terms to select vocab
        wordCounts.sortBy(_._2, ascending = false).take(vocabSize)
      }
      (tmpSortedWC.map(_._1).zipWithIndex.toMap, tmpSortedWC.map(_._2).sum)
    }

    val documents = tokenized.map { case (id, tokens) =>
      // Filter tokens by vocabulary, and create word count vector representation of document.
      val wc = new mutable.HashMap[Int, Int]()
      tokens.foreach { term =>
        if (vocab.contains(term)) {
          val termIndex = vocab(term)
          wc(termIndex) = wc.getOrElse(termIndex, 0) + 1
        }
      }
      val indices = wc.keys.toArray.sorted
      val values = indices.map(i => wc(i).toDouble)

      val sb = Vectors.sparse(vocab.size, indices, values)
      (id, sb)
    }

    val vocabArray = new Array[String](vocab.size)
    vocab.foreach { case (term, i) => vocabArray(i) = term }

    (documents.repartition(numPartitions), vocabArray, selectedTokenCount, textIdRDD)
  }

  private def getMedian(rdd: RDD[Double]): Double = {
    val sorted = rdd.sortBy(identity).zipWithIndex().map {
      case (v, idx) => (idx, v)
    }

    val count = sorted.count()
    val median: Double = if (count % 2 == 0) {
      val l = count / 2 - 1
      val r = l + 1
      (sorted.lookup(l).head + sorted.lookup(r).head) / 2
    } else sorted.lookup(count / 2).head

    median
  }
}

/**
  * Simple Tokenizer.
  *
  * TODO: Formalize the interface, and make this a public class in mllib.feature
  */
private class SimpleTokenizer(sc: SparkContext, stopwordFile: String) extends Serializable {

  private val stopwords: Set[String] = if (stopwordFile.isEmpty) {
    Set.empty[String]
  } else {
    val stopwordText = sc.textFile(stopwordFile).collect()
    stopwordText.flatMap(_.stripMargin.split("\\s+")).toSet
  }

  // Matches sequences of Unicode letters
  private val allWordRegex = "^(\\p{L}*)$".r

  // Ignore words shorter than this length.
  private val minWordLength = 3

  def getWords(text: String): IndexedSeq[String] = {

    val words = new mutable.ArrayBuffer[String]()

    // Use Java BreakIterator to tokenize text into words.
    val wb = BreakIterator.getWordInstance
    wb.setText(text)

    // current,end index start,end of each word
    var current = wb.first()
    var end = wb.next()
    while (end != BreakIterator.DONE) {
      // Convert to lowercase
      val word: String = text.substring(current, end).toLowerCase
      // Remove short words and strings that aren't only letters
      word match {
        case allWordRegex(w) if w.length >= minWordLength && !stopwords.contains(w) =>
          words += w
        case _ =>
      }

      current = end
      try {
        end = wb.next()
      } catch {
        case e: Exception =>
          // Ignore remaining text in line.
          // This is a known bug in BreakIterator (for some Java versions),
          // which fails when it sees certain characters.
          end = BreakIterator.DONE
      }
    }
    words
  }

}

// scalastyle:on println
