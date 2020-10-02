package nifi

import http.HttpClient.{ httpRequest, HTTP_DEL, HTTP_GET, HTTP_POST }
import http.{ HttpClient, KerberosHttpClient }
import org.apache.http.client.methods.{ CloseableHttpResponse, HttpGet, HttpPost, HttpRequestBase }
import org.apache.http.util.EntityUtils
import org.json.simple.JSONObject
import org.json.simple.parser.JSONParser

object NifiApiCaller {
  val PROCESS_GROUPS_URL        = "%s/nifi-api/flow/process-groups/%s"
  val PROCESS_URL               = "%s/nifi-api/processors/%s"
  val KERBEROS_ACCESS_TOKEN_URL = "%s/access/kerberos"
  val CLUSTER_SUMMARY_URL       = "%s/nifi-api/flow/cluster/summary"
  val SYSTEM_HEALTH_URL         = "%s/nifi-api/system-diagnostics"
  val GET_PROVENANCE_URL        = "%s/nifi-api/provenance/"
  val POST_PROVENANCE_URL       = "%s/nifi-api/provenance/%s"
  val PROCESS_HIST_URL          = "%s/nifi-api/flow/processors/%s/status/history"
  def getAccessToken(url: String, userId: String, principal: String, keyTabLocation: String): String = {
    val response =
      KerberosHttpClient.request(new HttpPost(KERBEROS_ACCESS_TOKEN_URL.format(url)), userId, principal, keyTabLocation)
    val entity = response.getEntity
    EntityUtils.toString(entity, "UTF-8")
  }

  def getClusterSummary(url: String, token: String = ""): String = {
    var headers = Map("content-type" -> "application/json")
    if (token.length > 0) {
      headers += ("Authorization" -> "Bearer %s".format(token))
    }
    HttpClient.httpRequest(
      http.HttpClient.HTTP_GET,
      CLUSTER_SUMMARY_URL.format(url),
      headers = headers
    )
  }
  def getProcessGroup(url: String, processGroupId: String, token: String = ""): String = {
    var headers = Map("content-type" -> "application/json")
    if (token.length > 0) {
      headers += ("Authorization" -> "Bearer %s".format(token))
    }
    HttpClient.httpRequest(
      http.HttpClient.HTTP_GET,
      PROCESS_GROUPS_URL.format(url, processGroupId),
      headers = headers
    )
  }

  def getProcess(url: String, processId: String, token: String = ""): String = {
    var headers = Map("content-type" -> "application/json")
    if (token.length > 0) {
      headers += ("Authorization" -> "Bearer %s".format(token))
    }
    HttpClient.httpRequest(
      http.HttpClient.HTTP_GET,
      PROCESS_URL.format(url, processId),
      headers = headers
    )
  }

  def getProvenance(url: String, json: String, token: String = ""): String = {
    import nifi.JSONFunctions._
    var headers = Map("content-type" -> "application/json")
    if (token.length > 0) {
      headers += ("Authorization" -> "Bearer %s".format(token))
    }
    val str = httpRequest(HTTP_POST, GET_PROVENANCE_URL.format(url), jsonData = json, headers = headers)

    val id = new JSONParser()
      .parse(str)
      .asInstanceOf[JSONObject]
      .getJson("provenance")
      .get("id")
      .asInstanceOf[String]
    val url2 = POST_PROVENANCE_URL.format(url, id)
    httpRequest(HTTP_DEL, url2, headers = Map("Content-Type" -> "application/json"))
    str
  }

  def getLineageDuration(url: String, processId: String, token: String = ""): Option[Long] = {
    import nifi.JSONFunctions._
    var headers = Map("content-type" -> "application/json")
    if (token.length > 0) {
      headers += ("Authorization" -> "Bearer %s".format(token))
    }
    val str = httpRequest(HTTP_GET, PROCESS_HIST_URL.format(url, processId), headers = headers)
    try {

      val history = new JSONParser()
        .parse(str)
        .asInstanceOf[JSONObject]
        .getJson("statusHistory")
        .getJsonArray("aggregateSnapshots")

      var maxtimestamp    = 0L
      var lineageDuration = 0L
      for (i <- 0 to history.size() - 1) {
        val status    = history.get(i).asInstanceOf[JSONObject]
        val timestamp = status.get("timestamp").asInstanceOf[Long]
        if (maxtimestamp < timestamp) {
          lineageDuration = status.getJson("statusMetrics").get("averageLineageDuration").asInstanceOf[Long]
          maxtimestamp = timestamp
        }
      }
      if ((System.currentTimeMillis() - maxtimestamp) < (1000 * 60 * 5)) {
        Option(lineageDuration)
      } else {
        Option.empty[Long]
      }
    } catch {
      case e => Option.empty[Long]
    }
  }
}
