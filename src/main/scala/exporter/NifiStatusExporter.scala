package exporter

import io.prometheus.client.Gauge
import io.prometheus.client.exporter.MetricsServlet
import nifi.{ NifiProcessorGraph, NifiProcessorStatus, NifiProcessorType }
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ ServletContextHandler, ServletHolder }

object NifiStatusExporter {
  val gauge = Gauge
    .build()
    .name("nifi_state")
    .help("nifi_processor_state")
    .labelNames(
      Array("process_group",
            "process_group_id",
            "flow_path",
            "flow_path_id",
            "processor_name",
            "id",
            "type",
            "flow_order",
            "properties"): _*
    )
    .register();

  def main(args: Array[String]): Unit = {

    //val mainProcessGroupId = "ab32d7bd-0174-1000-6691-72922f8a7f91"
    //val domain             = "http://localhost:8081"

    val mainProcessGroupId = args(0)
    val domain             = args(1)
    var crawlInterval      = 15000
    if (args.length > 2) {
      crawlInterval = args(2).toInt
    }

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
      //"process_group", "flow_path", "processor_name", "processor", "id", "type", "flow_order", "properties"

      val graph = NifiProcessorGraph.getGraph(mainProcessGroupId, domain).flattenGraph()

      graph
        .foreach(
          processChain => {

            var count = 1;
            processChain.foreach(processor => {
              val fields = processor.componentType match {
                case NifiProcessorType.FLOW_PROCESS_GROUP => {
                  Map(
                    "aggregateSnapshot_bytesRead"            -> "bytesRead",
                    "aggregateSnapshot_bytesWritten"         -> "bytesWritten",
                    "aggregateSnapshot_flowFilesIn"          -> "flowFilesIn",
                    "aggregateSnapshot_flowFilesOut"         -> "flowFilesOut",
                    "aggregateSnapshot_bytesIn"              -> "bytesIn",
                    "aggregateSnapshot_bytesOut"             -> "bytesOut",
                    "aggregateSnapshot_flowFilesTransferred" -> "flowFilesTransferred",
                    "aggregateSnapshot_flowFilesQueued"      -> "flowFilesQueued"
                  )
                }
                case NifiProcessorType.FLOW_CONNECTION => {
                  Map(
                    "aggregateSnapshot_percentUseCount" -> "percentUseCount",
                    "aggregateSnapshot_percentUseBytes" -> "percentUseBytes",
                    "aggregateSnapshot_flowFilesOut"    -> "flowFilesOut",
                    "aggregateSnapshot_flowFilesIn"     -> "flowFilesIn",
                    "aggregateSnapshot_bytesIn"         -> "bytesIn",
                    "aggregateSnapshot_bytesOut"        -> "bytesOut",
                    "aggregateSnapshot_flowFilesQueued" -> "flowFilesQueued"
                  )
                }
                case NifiProcessorType.FLOW_PROCESS => {
                  Map(
                    "aggregateSnapshot_bytesRead"          -> "bytesRead",
                    "aggregateSnapshot_bytesWritten"       -> "bytesWritten",
                    "aggregateSnapshot_flowFilesIn"        -> "flowFilesIn",
                    "aggregateSnapshot_flowFilesOut"       -> "flowFilesOut",
                    "aggregateSnapshot_taskCount"          -> "taskCount",
                    "aggregateSnapshot_tasksDurationNanos" -> "tasksDurationNanos"
                  )
                }
              }

              fields.foreach(field => {
                try {
                  NifiProcessorStatus.getCCParams(processor.status.get).get(field._1).get match {
                    case Some(x) if (x.isInstanceOf[Long] || x.isInstanceOf[Int]) => {
                      gauge
                        .labels(
                          processor.group.get.name,
                          processor.group.get.id,
                          processChain.head.name + "_To_" + processChain.last.name,
                          processChain.head.id + "_To_" + processChain.last.id,
                          processor.name,
                          processor.id,
                          processor.componentType,
                          count.toString,
                          field._2
                        )
                        .set(x.asInstanceOf[Long])
                    }
                  }
                } catch {
                  case e => {
                    println(field._1)
                    println(NifiProcessorStatus.getCCParams(processor.status.get).get(field._1).get)
                    println("%s Exception: %s ".format(processor.id, e.getMessage))

                    gauge
                      .labels(
                        processor.group.get.name,
                        processor.group.get.id,
                        processChain.head.name + "_To_" + processChain.last.name,
                        processChain.head.id + "_To_" + processChain.last.id,
                        processor.name,
                        processor.id,
                        processor.componentType,
                        count.toString,
                        field._2
                      )
                      .set(0)
                  }
                }
              })

              gauge
                .labels(
                  processor.group.get.name,
                  processor.group.get.id,
                  processChain.head.name + "_To_" + processChain.last.name,
                  processChain.head.id + "_To_" + processChain.last.id,
                  processor.name,
                  processor.id,
                  processor.componentType,
                  count.toString,
                  "runStatus"
                )
                .set(if (processor.status.get.runStatus.get.equalsIgnoreCase("running")) {
                  1
                } else {
                  0
                })

              count += 1
            })
          }
        )

      Thread.sleep(crawlInterval)
    }
  }
}
