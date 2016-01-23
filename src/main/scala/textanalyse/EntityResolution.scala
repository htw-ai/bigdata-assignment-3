package textanalyse

import java.util.Locale

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

class EntityResolution(sc: SparkContext, dat1: String, dat2: String, stopwordsFile: String, goldStandardFile: String) {

  val amazonRDD: RDD[(String, String)] = Utils.getData(dat1, sc)
  val googleRDD: RDD[(String, String)] = Utils.getData(dat2, sc)
  val stopWords: Set[String] = Utils.getStopWords(stopwordsFile)
  val goldStandard: RDD[(String, String)] = Utils.getGoldStandard(goldStandardFile, sc)

  var amazonTokens: RDD[(String, List[String])] = _
  var googleTokens: RDD[(String, List[String])] = _
  var corpusRDD: RDD[(String, List[String])] = _
  var idfDict: Map[String, Double] = _

  var idfBroadcast: Broadcast[Map[String, Double]] = _
  var similarities: RDD[(String, String, Double)] = _

  /**
    * getTokens soll die Funktion tokenize auf das gesamte RDD anwenden
    * und aus allen Produktdaten eines RDDs die Tokens extrahieren.
    */
  def getTokens(data: RDD[(String, String)]): RDD[(String, List[String])] = {
    val sw = stopWords
    //   val stopWords: Set[String] = Utils.getStopWords(stopwordsFile)
    data.map(x => {
      (x._1, EntityResolution.tokenize(x._2, sw))
    })
  }

  /**
    * Zählt alle Tokens innerhalb eines RDDs
    * Duplikate sollen dabei nicht eliminiert werden
    */
  def countTokens(data: RDD[(String, List[String])]): Long = {
    data.map(x => x._2.size).sum().toLong
  }

  /**
    * Findet den Datensatz mit den meisten Tokens
    */
  def findBiggestRecord(data: RDD[(String, List[String])]): (String, List[String]) = {
    data.fold("dummy", List[String]())((acc, x) => if (acc._2.size > x._2.size) acc else x)
  }

  /**
    * Erstellt die Tokenmenge für die Amazon und die Google-Produkte
    * (amazonRDD und googleRDD) und vereinigt diese und speichert das
    * Ergebnis in corpusRDD
    */
  def createCorpus = {
    amazonTokens = getTokens(amazonRDD)
    googleTokens = getTokens(googleRDD)
    corpusRDD = (amazonTokens union googleTokens).reduceByKey(_ ++ _)
  }

  /**
    * Berechnung des IDF-Dictionaries auf Basis des erzeugten Korpus
    * Speichern des Dictionaries in die Variable idfDict
    */
  def calculateIDF = {
    val count = corpusRDD.count().asInstanceOf[Double]
    var dict = Map.empty[String, Int]

    corpusRDD.map(_._2.toSet).toLocalIterator.toList.foreach(s => {
      s.foreach(t => dict = dict + (t -> (dict.getOrElse[Int](t, 0) + 1)))
    })
    idfDict = dict.map(x => (x._1, count / x._2))

    idfBroadcast = sc.broadcast(idfDict)
  }

  /**
    * Berechnung der Document-Similarity für alle möglichen
    * Produktkombinationen aus dem amazonRDD und dem googleRDD
    * Ergebnis ist ein RDD aus Tripeln bei dem an erster Stelle die AmazonID
    * steht, an zweiter die GoogleID und an dritter der Wert
    */
  def simpleSimimilarityCalculation: RDD[(String, String, Double)] = {
    createCorpus
    calculateIDF
    val sw = stopWords
    val idf = idfDict
    amazonRDD
      .cartesian(googleRDD)
      .map(x => EntityResolution.computeSimilarity(x, idf, sw))
  }

  /**
    * Funktion zum Finden des Similarity-Werts für zwei ProduktIDs
    */
  def findSimilarity(vendorID1: String, vendorID2: String, sim: RDD[(String, String, Double)]): Double = {
    val d = sim
      .filter(x => x._1 == vendorID1 && x._2 == vendorID2)
      .take(1)
    d(0)._3
  }

  def simpleSimimilarityCalculationWithBroadcast: RDD[(String, String, Double)] = {
    val sw = stopWords
    val idf = idfBroadcast.value

    amazonRDD.cartesian(googleRDD)
      .map(x => EntityResolution.computeSimilarity(x, idf, sw))
  }

  /*
   *
   * 	Gold Standard Evaluation
   */


  /**
    * Berechnen Sie die folgenden Kennzahlen:
    *
    * Anzahl der Duplikate im Sample
    * Durchschnittliche Consinus Similaritaet der Duplikate
    * Durchschnittliche Consinus Similaritaet der Nicht-Duplikate
    *
    *
    * Ergebnis-Tripel:
    * (AnzDuplikate, avgCosinus-SimilaritätDuplikate,avgCosinus-SimilaritätNicht-Duplikate)
    */
  def evaluateModel(goldStandard: RDD[(String, String)]): (Long, Double, Double) = {

    val myGoldStandard = goldStandard.map(x => {
      val keys = x._1.split(" ")
      (keys(0), keys(1))
    }).collect().toList

    val sw = stopWords
    val idf = idfDict

    val p = amazonRDD.cartesian(googleRDD).map(x => EntityResolution.computeSimilarity((x._1, x._2), idf, sw) )

    val dups = p.filter(x => myGoldStandard.contains((x._1, x._2)))
    val noDups = p.filter(x => !myGoldStandard.contains((x._1, x._2)))

    val sumDups = dups.map(_._3).sum()
    val sumNoDups = noDups.map(_._3).sum()

    (dups.count, sumDups / dups.count.toDouble, sumNoDups / noDups.count.toDouble)
  }
}


object EntityResolution {

  /**
    * Tokenize splittet einen String in die einzelnen Wörter auf
    * und entfernt dabei alle Stopwords.
    * Verwenden Sie zum Aufsplitten die Methode Utils.tokenizeString
    */
  def tokenize(s: String, stopws: Set[String]): List[String] = {
    Utils.tokenizeString(s).filterNot(x => stopws.contains(x.toLowerCase(Locale.ROOT)))
  }

  /**
    * Berechnet die Relative Haeufigkeit eine Wortes in Bezug zur
    * Menge aller Wörter innerhalb eines Dokuments
    */
  def getTermFrequencies(tokens: List[String]): Map[String, Double] = {
    val singleAppearance = 1 / tokens.size.toDouble

    tokens.foldLeft(Map[String, Double]())({
      (m, x) => m + x.->(m.getOrElse[Double](x, 0) + singleAppearance)
    })
  }

  /**
    * Bererechnung der Document-Similarity einer Produkt-Kombination
    * Rufen Sie in dieser Funktion calculateDocumentSimilarity auf, in dem
    * Sie die erforderlichen Parameter extrahieren
    */
  def computeSimilarity(record: ((String, String), (String, String)), idfDictionary: Map[String, Double], stopWords: Set[String]): (String, String, Double) = {
    (record._1._1, record._2._1, calculateDocumentSimilarity(record._1._2, record._2._2, idfDictionary, stopWords))
  }

  /**
    * Berechnung von TF-IDF Wert für eine Liste von Wörtern
    * Ergebnis ist eine Mapm die auf jedes Wort den zugehörigen TF-IDF-Wert mapped
    */
  def calculateTF_IDF(terms: List[String], idfDictionary: Map[String, Double]): Map[String, Double] = {
    getTermFrequencies(terms).map(x => (x._1, x._2 * idfDictionary.getOrElse[Double](x._1, 0)))
  }

  /**
    * Berechnung des Dot-Products von zwei Vectoren
    */
  def calculateDotProduct(v1: Map[String, Double], v2: Map[String, Double]): Double = {
    v1.map(x => x._2 * v2.getOrElse[Double](x._1, 0))
      .sum
  }

  /**
    * Berechnung der Norm eines Vectors
    */
  def calculateNorm(vec: Map[String, Double]): Double = {
    Math.sqrt(((for (el <- vec.values) yield el * el).sum))
  }

  /**
    * Berechnung der Cosinus-Similarity für zwei Vectoren
    */
  def calculateCosinusSimilarity(doc1: Map[String, Double], doc2: Map[String, Double]): Double = {
    calculateDotProduct(doc1, doc2) / (calculateNorm(doc1) * calculateNorm(doc2))
  }

  /**
    * Berechnung der Document-Similarity für ein Dokument
    */
  def calculateDocumentSimilarity(doc1: String, doc2: String, idfDictionary: Map[String, Double], stopWords: Set[String]): Double = {
    calculateCosinusSimilarity(
      calculateTF_IDF(tokenize(doc1, stopWords), idfDictionary),
      calculateTF_IDF(tokenize(doc2, stopWords), idfDictionary))
  }

  /**
    * Bererechnung der Document-Similarity einer Produkt-Kombination
    * Rufen Sie in dieser Funktion calculateDocumentSimilarity auf, in dem
    * Sie die erforderlichen Parameter extrahieren
    * Verwenden Sie die Broadcast-Variable.
    */
  def computeSimilarityWithBroadcast(record: ((String, String), (String, String)), idfBroadcast: Broadcast[Map[String, Double]], stopWords: Set[String]): (String, String, Double) = {

    (record._1._1, record._2._1, calculateDocumentSimilarity(record._1._2, record._2._2, idfBroadcast.value, stopWords))
  }
}
