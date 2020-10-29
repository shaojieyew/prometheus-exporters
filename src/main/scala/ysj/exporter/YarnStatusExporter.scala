package ysj.exporter

import io.circe.{Json, parser}
import io.prometheus.client.Gauge
import io.prometheus.client.exporter.MetricsServlet
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClientBuilder
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import ysj.http.KerberosHttpClient.request

object YarnStatusExporter {
  val gauge = Gauge
    .build()
    .name("spark_app_state")
    .help("Number of spark app running by application name")
    .labelNames(Array("app_name"): _*)
    .register();
  /*
  http://localhost:8088/ws/v1/cluster/apps?states=running,accepted&applicationTypes=Spark
  http://localhost:8088/ws/v1/cluster/appstatistics?states=running&applicationTypes=Spark
  http://localhost:8088/ws/v1/cluster/nodes/
   */
  def main(args: Array[String]): Unit = {

    val kerberosEnable = false
    val userid         = "HTTP/test@test.com"
    val principle      = "HTTP/test@test.com"
    val keytabLocation = "file://C://Development//test.keytab"
    val url            = "http://localhost:8088/ws/v1/cluster/apps?states=running,accepted&applicationTypes=Spark"

    new Thread(new Runnable {
      override def run(): Unit = {
        val server  = new Server(1234)
        val context = new ServletContextHandler()
        context.setContextPath("/")
        server.setHandler(context)
        context.addServlet(new ServletHolder(new MetricsServlet()), "/metrics")
        server.start()
        server.join()
      }
    }).start()

    while (true) {

      val httpGetRequest = new HttpGet(url)
      val response = if (kerberosEnable) {
        request(new HttpGet(url), userid, principle, keytabLocation)
      } else {
        HttpClientBuilder.create.build().execute(httpGetRequest)
      }

      val inputStream = response.getEntity.getContent
      val result      = new String(IOUtils.toByteArray(inputStream), "UTF-8")

      val json: Json        = parser.parse(result).right.getOrElse(Json.Null)
      var app: Option[Json] = None
      var index: Int        = 0

      var runningList = Map[String, Int]()

      do {
        val jsonApp = json.hcursor.downField("apps").downField("app").downN(index)
        app = jsonApp.focus

        if (app.isDefined) {
          val name = jsonApp.get[String]("name").right.getOrElse("")
          if (name.length > 0) {
            runningList += (name -> (runningList.get(name).getOrElse(0) + 1))
          }
        }
        index += 1
      } while (app.isDefined)

      runningList.map(app => {
        gauge.labels(app._1).set(app._2)
      })
      Thread.sleep(1000)
    }
  }
}
