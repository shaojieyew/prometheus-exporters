package ysj.example

import io.prometheus.client.Summary
import io.prometheus.client.exporter.MetricsServlet
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}

object SummaryExporter {
  val receivedBytes = Summary
    .build()
    .name("requests_size_bytes")
    .help("Request size in bytes.")
    .register();
  val requestLatency = Summary
    .build()
    .name("requests_latency_seconds")
    .help("Request latency in seconds.")
    .labelNames("time")
    .register();
  val requestLatency2 = Summary
    .build()
    .quantile(0.5, 0.05) // Add 50th percentile (= median) with 5% tolerated error
    .quantile(0.9, 0.01) // Add 90th percentile with 1% tolerated error
    .name("requests_latency_seconds_50_90_percentil")
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
      val requestTimer  = requestLatency.labels((System.currentTimeMillis() / 1000).toString).startTimer();
      val requestTimer2 = requestLatency2.startTimer();

      receivedBytes.observe(Math.random() * 10);
      Thread.sleep(1000)
      requestTimer.observeDuration();
      requestTimer2.observeDuration();
      println(System.currentTimeMillis())
    }
  }
}
