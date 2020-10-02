package exporter

import http.HttpClient
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import io.prometheus.client.Gauge
import io.prometheus.client.exporter.MetricsServlet
import nifi.entity.NifiProcessorStatus
import nifi.{ NifiApiCaller, NifiProcessorGraphBuilder, NifiProcessorType, NifiProvenanceQuery }
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ ServletContextHandler, ServletHolder }
import org.json.simple.JSONObject
import org.json.simple.parser.JSONParser

object NifiStatusExporter {
  val latencyGauge = Gauge
    .build()
    .name("nifi_flow_latency")
    .help("measure of latency in a chain of processors")
    .labelNames(
      Array("process_group", "process_group_id", "flow_path", "flow_path_id", "processor_name", "id"): _*
    )
    .register();
  val backlogGauge = Gauge
    .build()
    .name("nifi_flow_backlog")
    .help("input folder's backlog")
    .labelNames(
      Array("process_group", "process_group_id", "processor_name", "id"): _*
    )
    .register()
  val flowRunStateGauge = Gauge
    .build()
    .name("nifi_flow_running_state")
    .help("running state of a flow path")
    .labelNames(
      Array("process_group", "process_group_id", "flow_path", "flow_path_id", "processor_name", "id"): _*
    )
    .register()
  val connectedNodeGauge = Gauge
    .build()
    .name("nifi_connected_node_count")
    .help("number of node connected")
    .register()
  val flowCountGauge = Gauge
    .build()
    .name("nifi_flow_count")
    .help("number of record published to kafka, and number of files fetched")
    .labelNames(
      Array("process_group", "process_group_id", "flow_path", "flow_path_id", "processor_name", "id", "type"): _*
    )
    .register()

  def getHDFSDirectorySize(conf: Configuration, directory: String): Long = {
    val fs   = FileSystem.get(conf)
    val path = new Path(directory)
    fs.getContentSummary(path).getLength
  }
  def main(args: Array[String]): Unit = {

    val mainProcessGroupId = "ab32d7bd-0174-1000-6691-72922f8a7f91"
    val url                = "http://localhost:8081"

    //val mainProcessGroupId = args(0)
    //val url             = args(1)
    var crawlInterval = 15000
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

    import nifi.JSONFunctions._
    while (true) {
      val token = ""
      val str   = NifiApiCaller.getClusterSummary(url, token)
      // println(PROCESS_URL.format(host, list_processor.head.id))

      val clusterSummary = new JSONParser()
        .parse(str)
        .asInstanceOf[JSONObject]
        .get("clusterSummary")
        .asInstanceOf[JSONObject]
        .clusterSummaryParser

      if (clusterSummary.clustered) {
        connectedNodeGauge.set(clusterSummary.connectedNodeCount)
      }

      val processChains = NifiProcessorGraphBuilder.getGraph(mainProcessGroupId, url).flattenGraph()
      processChains
        .foreach(
          processChain => {
            // start of export backlog
            // exportEtaBacklog(processChain, url)
            // end of export backlog

            var processorOrderIndex = 1;
            var running             = true
            processChain.foreach(processor => {
              if (processor.componentType.equals(NifiProcessorType.FLOW_PROCESS)) {
                // start of export latency
                exportLatency(processor, processChain, processorOrderIndex, url, token)
                // end of export latency
                val runningStatus = processor.status.getOrElse(NifiProcessorStatus()).runStatus

                // start of export
                if (runningStatus.getOrElse("") != "Running") {
                  running = false
                }
                val chainId      = processChain.head.id + "->" + processor.id
                val chainName    = processChain.head.name + "->" + processor.name
                val processGroup = processChain.head.group
                flowRunStateGauge
                  .labels(
                    processor.group.get.name,
                    processor.group.get.id,
                    chainName,
                    chainId,
                    processor.name,
                    processor.id
                  )
                  .set(if (running) 1 else 0)
                // end of export

                val flowfileIn   = processor.status.getOrElse(NifiProcessorStatus()).flowFilesIn
                val flowFilesOut = processor.status.getOrElse(NifiProcessorStatus()).flowFilesOut
                val bytesRead    = processor.status.getOrElse(NifiProcessorStatus()).bytesRead
                val bytesWritten = processor.status.getOrElse(NifiProcessorStatus()).bytesWritten
                if (flowfileIn.isDefined) {
                  flowCountGauge
                    .labels(
                      processor.group.get.name,
                      processor.group.get.id,
                      chainName,
                      chainId,
                      processor.name,
                      processor.id,
                      "flowfileIn"
                    )
                    .set(flowfileIn.get)
                }
                if (flowFilesOut.isDefined) {
                  flowCountGauge
                    .labels(
                      processor.group.get.name,
                      processor.group.get.id,
                      chainName,
                      chainId,
                      processor.name,
                      processor.id,
                      "flowFilesOut"
                    )
                    .set(flowFilesOut.get)
                }
                if (bytesRead.isDefined) {
                  flowCountGauge
                    .labels(
                      processor.group.get.name,
                      processor.group.get.id,
                      chainName,
                      chainId,
                      processor.name,
                      processor.id,
                      "bytesRead"
                    )
                    .set(bytesRead.get)
                }
                if (bytesWritten.isDefined) {
                  flowCountGauge
                    .labels(
                      processor.group.get.name,
                      processor.group.get.id,
                      chainName,
                      chainId,
                      processor.name,
                      processor.id,
                      "bytesWritten"
                    )
                    .set(bytesWritten.get)
                }

                processorOrderIndex += 1
              }
            })

          }
        )
      Thread.sleep(crawlInterval)
    }
  }

  def exportBacklog(processChain: Array[NifiProcessorGraphBuilder.NifiProcessor], domain: String): Unit =
    if (processChain.head.componentType == NifiProcessorType.FLOW_PROCESS) {
      val processors = processChain
        .filter(_.componentType.equalsIgnoreCase(NifiProcessorType.FLOW_PROCESS))
      val list_processor = processors
        .filter(processor => {
          processor.name.toUpperCase.contains("HDFS") && processor.name.toUpperCase.contains("LIST")
        })

      val fetch_processor = processors
        .filter(processor => {
          processor.name.toUpperCase.contains("HDFS") && processor.name.toUpperCase.contains("FETCH")
        })

      if (list_processor.length > 0 && fetch_processor.length > 0) {
        val str = NifiApiCaller.getProcess(domain, list_processor.head.id)

        // println(PROCESS_URL.format(host, list_processor.head.id))
        val properties = new JSONParser()
          .parse(str)
          .asInstanceOf[JSONObject]
          .get("component")
          .asInstanceOf[JSONObject]
          .get("config")
          .asInstanceOf[JSONObject]
          .get("properties")
          .asInstanceOf[JSONObject]
        val path = properties
          .get("Input Directory")
          .asInstanceOf[String]
        val backlogByte = getHDFSDirectorySize(new Configuration(), path).toFloat
        backlogGauge
          .labels(
            processChain.head.group.get.name,
            processChain.head.group.get.id,
            list_processor.head.name,
            list_processor.head.id,
            (processChain.indexOf(list_processor) + 1).toString
          )
          .set(backlogByte)
      }
    }

  var lastKnownMinimumLatencies = Map[String, Long]()
  def exportLatency(processor: NifiProcessorGraphBuilder.NifiProcessor,
                    processChain: Array[NifiProcessorGraphBuilder.NifiProcessor],
                    processorOrderIndex: Int,
                    domain: String,
                    token: String = ""): Unit = {

    val chainId   = processChain.head.id + "->" + processor.id
    val chainName = processChain.head.name + "->" + processor.name
    val lineage   = NifiApiCaller.getLineageDuration(domain, processor.id)
    if (lineage.isDefined) {
      latencyGauge
        .labels(
          processor.group.get.name,
          processor.group.get.id,
          chainName,
          chainId,
          processor.name,
          processor.id
        )
        .set(lineage.get)
    }
  }
}
