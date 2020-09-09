package example

import io.prometheus.client.Counter
import io.prometheus.client.exporter.MetricsServlet
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ ServletContextHandler, ServletHolder }

object CounterExporter {
  val requests = Counter
    .build()
    .name("requests_total")
    .help("Total requests.")
    .labelNames(Array("labelNames1", "labelNames2"): _*)
    .register();

  def main(args: Array[String]): Unit = {
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
      requests.labels("labelA", "A").inc()
      requests.labels("labelA", "B").inc()
      requests.labels("labelB", "B").inc()
      Thread.sleep(1000)
      println(System.currentTimeMillis())
    }
  }
}
