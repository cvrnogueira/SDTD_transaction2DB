package io.sdtd.processors

import io.sdtd.TwitterPayload
import io.sdtd.helpers.Helpers
import org.ahocorasick.trie.Trie
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.JavaConverters._

object TwitterWindowedGroupCount {

  val EmptyLocationIdentifier = "Empty"

  lazy val dictionary = Trie
    .builder()
    .addKeywords(DiseaseWords.all.asJava)
    .ignoreCase()
    .stopOnHit()
    .build()

  def filter(sourceStream: DataStream[String]): DataStream[TwitterPayload] = {
    sourceStream

      // consumes and convert string tweet to
      // a tweet object
      .map(s => Helpers.convertToTwitterPayload(s))

      // tweets to be processed
      .filter(_.isDefined)
      .map(_.get)
  }

  def pipeGrouped(sourceStream: DataStream[String]): DataStream[TwitterPayload] = {
    filter(sourceStream)

      // tweets can have no location at all or
      // custom locations then we filter non-empty location
      .filter(t => EmptyLocationIdentifier != t.location)

      // fast algorithm for searching through a word dictionary
      // https://cr.yp.to/bib/1975/aho.pdf
      .filter(t => dictionary.containsMatch(t.tweet))

      // keep a time window buffer with all tweets grouped by
      // location and once this window times count grouped length
      .keyBy("location")
      .timeWindow(Time.seconds(5))
      .sum("counter")
  }

  @inline
  def pipeSingle(sourceStream: DataStream[String]): DataStream[(Long, Long)] = {
    filter(sourceStream)

      .timeWindowAll(Time.seconds(1))

      .apply{ (context, elements, out) =>
          out.collect((elements.size.toLong, context.maxTimestamp / 1000))
      }
  }
}

object DiseaseWords {

  private val traditionalChinese = List(
    "\u6D41\u611F",
    "\u6D41\u611F",
    "\u767C\u51B7",
    "\u982D\u75DB",
    "\u5589\u56A8\u75DB",
    "\u6D41\u9F3B\u6D95",
    "\u6253\u5674\u568F",
    "\u767C\u71D2",
    "\u4E7E\u54B3",
    "\u54B3\u55FD"
  )

  private val simplifiedChinese = List(
    "\u6D41\u611F",
    "\u6D41\u611F",
    "\u53D1\u51B7",
    "\u5934\u75DB",
    "\u54BD\u5589\u75DB",
    "\u6D41\u9F3B\u6D95",
    "\u6253\u55B7\u568F",
    "\u53D1\u70ED",
    "\u5E72\u54B3",
    "\u54B3\u55FD"
  )

  val chinese = traditionalChinese ++ simplifiedChinese

  val english = List("Influenza", "flu", "chills", "headache", "sore throat", "runny nose", "sneezing", "fever", "dry cough", "cough")

  val hindi = List(
    "\u0907\u0902\u092B\u094D\u0932\u0941\u090F\u0902\u091C\u093E",
    "\u092B\u093C\u094D\u0932\u0942",
    "\u0920\u0902\u0921 \u0932\u0917\u0928\u093E",
    "\u0938\u0930\u0926\u0930\u094D\u0926",
    "\u0917\u0932\u0947 \u092E\u0947\u0902 \u0916\u0930\u093E\u0936",
    "\u092C\u0939\u0924\u0940 \u0928\u093E\u0915",
    "\u091B\u0940\u0902\u0915 \u0906\u0928\u093E",
    "\u092C\u0941\u0916\u093E\u0930",
    "\u0938\u0942\u0916\u0940 \u0916\u093E\u0901\u0938\u0940",
    "\u0916\u093E\u0902\u0938\u0940"
  )

  val spanish = List(
    "Influenza",
    "gripe",
    "escalofríos",
    "dolor de cabeza",
    "dolor de garganta",
    "nariz que moquea",
    "estornudos",
    "fiebre",
    "tos seca",
    "tos"
  )

  val arabic = List(
    "\u0625\u0646\u0641\u0644\u0648\u0646\u0632\u0627",
    "\u0623\u0646\u0641\u0644\u0648\u0646\u0632\u0627",
    "\u0642\u0634\u0639\u0631\u064A\u0631\u0629 \u0628\u0631\u062F",
    "\u0635\u062F\u0627\u0639 \u0627\u0644\u0631\u0627\u0633",
    "\u0625\u0644\u062A\u0647\u0627\u0628 \u0627\u0644\u062D\u0644\u0642",
    "\u0633\u064A\u0644\u0627\u0646 \u0627\u0644\u0623\u0646\u0641",
    "\u0627\u0644\u0639\u0637\u0633",
    "\u062D\u0645\u0649",
    "\u0633\u0639\u0627\u0644 \u062C\u0627\u0641",
    "\u0633\u0639\u0627\u0644"
  )

  val portuguese = List(
    "gripe",
    "dor de cabe\u00E7a",
    "arrepios",
    "dor de garganta",
    "coriza",
    "espirros",
    "febre",
    "tosse seca",
    "tosse"
  )

  val french = List(
    "grippe",
    "frissons",
    "maux de t\u00EAte",
    "gorge irrit\u00E9e",
    "nez qui coule",
    "\u00E9ternuements",
    "fi\u00E8vre",
    "toux s\u00E8che",
    "la toux"
  )

  val german = List(
    "Sch\u00FCttelfrost",
    "Kopfschmerzen",
    "Halsentz\u00FCndung",
    "laufende Nase",
    "Niesen",
    "Fieber",
    "trockener Husten",
    "Husten"
  )

  val russian = List(
    "\u0433\u0440\u0438\u043F\u043F",
    "\u0433\u0440\u0438\u043F\u043F",
    "\u043E\u0437\u043D\u043E\u0431",
    "\u0413\u043E\u043B\u043E\u0432\u043D\u0430\u044F \u0431\u043E\u043B\u044C",
    "\u0431\u043E\u043B\u044C\u043D\u043E\u0435 \u0433\u043E\u0440\u043B\u043E",
    "\u043D\u0430\u0441\u043C\u043E\u0440\u043A",
    "\u0447\u0438\u0445\u0430\u043D\u0438\u0435",
    "\u043B\u0438\u0445\u043E\u0440\u0430\u0434\u043A\u0430",
    "\u0441\u0443\u0445\u043E\u0439 \u043A\u0430\u0448\u0435\u043B\u044C",
    "\u043A\u0430\u0448\u0435\u043B\u044C"
  )

  val japanese = List(
    "\u30A4\u30F3\u30D5\u30EB\u30A8\u30F3\u30B6",
    "\u30A4\u30F3\u30D5\u30EB\u30A8\u30F3\u30B6",
    "\u5BD2\u6C17",
    "\u982D\u75DB",
    "\u5589\u306E\u75DB\u307F",
    "\u9F3B\u6C34",
    "\u304F\u3057\u3083\u307F",
    "\u71B1",
    "\u4E7E\u3044\u305F\u54B3",
    "\u54B3"
  )

  val all = chinese ++
    english ++
    german ++ french ++
    hindi ++
    arabic ++
    japanese ++
    portuguese ++
    russian ++
    spanish
}