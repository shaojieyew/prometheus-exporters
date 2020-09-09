package example

import io.prometheus.client.Histogram
import io.prometheus.client.exporter.MetricsServlet
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ ServletContextHandler, ServletHolder }

object HistogramExporter {
  val requestLatency = Histogram
    .build()
    .name("requests_latency_seconds")
    .help("Request latency in seconds.")
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
      val requestTimer = requestLatency.startTimer();

      requestTimer.observeDuration();
      Thread.sleep(1000)
      println(System.currentTimeMillis())
    }
  }
}
