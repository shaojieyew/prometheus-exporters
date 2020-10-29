package ysj.nifi

import org.json.simple.JSONObject
import org.json.simple.parser.JSONParser
import ysj.nifi.JSONFunctions._
import ysj.nifi.entity.NifiProvenanceEvents
object NifiProvenanceQuery {

  def queryProvenanceSummary(url: String,
                             processorId: String,
                             head: Int = 1,
                             token: String = ""): Array[NifiProvenanceEvents] = {
    val json =
      s"""
        |{
        |  "provenance": {
        |    "request": {
        |      "maxResults": $head,
        |      "summarize": false,
        |      "incrementalResults": true,
        |      "searchTerms": {
        |        "ProcessorID": "$processorId"
        |      }
        |    }
        |  }
        |}
      """.stripMargin

    val str = NifiApiCaller.getProvenance(url, json, token)

    var arr = Array[NifiProvenanceEvents]()
    val provenanceEvents = new JSONParser()
      .parse(str)
      .asInstanceOf[JSONObject]
      .getJson("provenance")
      .getJson("results")
      .getJsonArray("provenanceEvents")

    for (i <- 0 to provenanceEvents.size() - 1) {
      arr = arr ++ Array(
        provenanceEvents
          .get(i)
          .asInstanceOf[JSONObject]
          .provenanceParser()
      )
    }
    arr
  }
}
