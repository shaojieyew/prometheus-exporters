package example

import io.prometheus.client.Gauge
import io.prometheus.client.exporter.MetricsServlet
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ ServletContextHandler, ServletHolder }

object GaugeExporter {
  val requests = Gauge
    .build()
    .name("inprogress_requests")
    .help("Inprogress requests.")
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
      if (Math.random() < 0.5D) {
        requests.inc()
      } else {
        requests.dec()
      }

      Thread.sleep(1000)
      println(System.currentTimeMillis())
    }
  }
}
