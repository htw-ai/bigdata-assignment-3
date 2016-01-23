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
    corpusRDD = getTokens((amazonRDD union googleRDD).reduceByKey(_ ++ _))
  }

  /**
    * Berechnung des IDF-Dictionaries auf Basis des erzeugten Korpus
    * Speichern des Dictionaries in die Variable idfDict
    */
  def calculateIDF2 = {
    val count = corpusRDD.count().asInstanceOf[Double]
    val dict = Map.empty[String, Int]

    corpusRDD.foreach((doc) => {
      doc._2.distinct.foreach(t =>
        dict.updated(t, if (dict.contains(t)) dict.apply(t) else 1)
//        dict + (t -> (if (dict.contains(t)) dict.apply(t) else 1))
      )
    })
    dict.foreach(x => idfDict ++ x._1 -> count / x._2.asInstanceOf[Double])

    idfBroadcast = sc.broadcast(idfDict)
  }

  def calculateIDF = {
    val documentCount = corpusRDD.count().asInstanceOf[Double]
    val uniqueWordsPerDocument = corpusRDD.map(_._2.toSet).toLocalIterator.toList

    //     BAD
    //    var map: Map[String, Long] = Map.empty
    //    uniqueWordsPerDocument.foreach(x => x.foreach(word => map = map.updated(word, 1 + map.getOrElse[Long](word, 0))))
    //    idfDict = map.map(x => (x._1, x._2 / documentCount.asInstanceOf[Double]))

    //     BETTER
    idfDict = uniqueWordsPerDocument
      .foldLeft(Map.empty[String, Long]) {
        (map, set) =>
          set.foldLeft(map) {
            (map2, word) => map2.updated(word, 1 + map2.getOrElse[Long](word, 0))
          }
      }
      .map(x => (x._1, x._2 / documentCount))

    idfBroadcast = sc.broadcast(idfDict)
  }

  /**
    * Berechnung der Document-Similarity für alle möglichen
    * Produktkombinationen aus dem amazonRDD und dem googleRDD
    * Ergebnis ist ein RDD aus Tripeln bei dem an erster Stelle die AmazonID
    * steht, an zweiter die GoogleID und an dritter der Wert
    */
  def simpleSimimilarityCalculation: RDD[(String, String, Double)] = {
    ???
  }

  def findSimilarity(vendorID1: String, vendorID2: String, sim: RDD[(String, String, Double)]): Double = {

    /*
     * Funktion zum Finden des Similarity-Werts für zwei ProduktIDs
     */
    ???
  }

  def simpleSimimilarityCalculationWithBroadcast: RDD[(String, String, Double)] = {

    ???
  }

  /*
   *
   * 	Gold Standard Evaluation
   */

  def evaluateModel(goldStandard: RDD[(String, String)]): (Long, Double, Double) = {

    /*
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


    ???
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
    val singleAppearance = (1 / tokens.size.toDouble).toDouble
    tokens.foldLeft(Map[String, Double]())({
      (m, x) => m + x.->(m.getOrElse[Double](x, 0) + singleAppearance)
    })
  }

  def computeSimilarity(record: ((String, String), (String, String)), idfDictionary: Map[String, Double], stopWords: Set[String]): (String, String, Double) = {
    /*
     * Bererechnung der Document-Similarity einer Produkt-Kombination
     * Rufen Sie in dieser Funktion calculateDocumentSimilarity auf, in dem 
     * Sie die erforderlichen Parameter extrahieren
     */
    ???
  }

  def calculateTF_IDF(terms: List[String], idfDictionary: Map[String, Double]): Map[String, Double] = {

    /* 
     * Berechnung von TF-IDF Wert für eine Liste von Wörtern
     * Ergebnis ist eine Mapm die auf jedes Wort den zugehörigen TF-IDF-Wert mapped
     */

    ???
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
    calculateDotProduct(doc1, doc2) / (calculateNorm(doc1) * calculateNorm(doc2 ))
  }

  def calculateDocumentSimilarity(doc1: String, doc2: String, idfDictionary: Map[String, Double], stopWords: Set[String]): Double = {

    /*
     * Berechnung der Document-Similarity für ein Dokument
     */
    ???
  }

  def computeSimilarityWithBroadcast(record: ((String, String), (String, String)), idfBroadcast: Broadcast[Map[String, Double]], stopWords: Set[String]): (String, String, Double) = {

    /*
     * Bererechnung der Document-Similarity einer Produkt-Kombination
     * Rufen Sie in dieser Funktion calculateDocumentSimilarity auf, in dem 
     * Sie die erforderlichen Parameter extrahieren
     * Verwenden Sie die Broadcast-Variable.
     */
    ???
  }
}
