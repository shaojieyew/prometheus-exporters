package ysj.http

import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods._
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils

object HttpClient {

  val HTTP_DEL  = "DEL"
  val HTTP_POST = "POST"
  val HTTP_PUT  = "PUT"
  val HTTP_GET  = "GET"
  def httpRequest(method: String,
                  url: String,
                  jsonData: String = null,
                  headers: Map[String, String] = Map()): String = {
    val request = method match {
      case HTTP_GET => new HttpGet(url)
      case post if (post == HTTP_POST || post == HTTP_PUT) => {
        val prequest = post match {
          case HTTP_PUT => {
            new HttpPut(url)
          }
          case HTTP_POST => {
            new HttpPost(url)
          }
        }
        if (jsonData != null) {
          prequest.setEntity(new StringEntity(jsonData))
        }
        prequest
      }
      case HTTP_DEL => new HttpDelete(url)
    }
    val timeout = 1800
    val requestConfig = RequestConfig
      .custom()
      .setConnectTimeout(timeout * 1000)
      .setConnectionRequestTimeout(timeout * 1000)
      .setSocketTimeout(timeout * 1000)
      .build()
    val client = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build()
    for (m <- headers) {
      request.addHeader(m._1, m._2)
    }
    val response: CloseableHttpResponse = client.execute(request)
    val entity                          = response.getEntity
    EntityUtils.toString(entity, "UTF-8")
  }
}
