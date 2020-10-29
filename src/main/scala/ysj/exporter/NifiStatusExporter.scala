package ysj.exporter

import io.prometheus.client.Gauge
import io.prometheus.client.exporter.MetricsServlet
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.json.simple.JSONObject
import org.json.simple.parser.JSONParser
import ysj.nifi.NifiProcessorGraphBuilder.NifiProcessor
import ysj.nifi.NifiProcessorType.FLOW_CONNECTION
import ysj.nifi.entity.NifiProcessorStatus
import ysj.nifi.{NifiApiCaller, NifiProcessorGraphBuilder, NifiProcessorType}

import scala.::
import scala.ysj.exporter.NifiStatusExporter1.flowCountGauge

object NifiStatusExporter {
  val connectedNodeGauge = Gauge
    .build()
    .name("nifi_connected_node_count")
    .help("number of node connected")
    .register()

  val flowRunStateGauge = Gauge
    .build()
    .name("nifi_flow_running_state")
    .help("running state of a flow path")
    .labelNames(
      Array("process_group", "process_group_id", "starting_process", "starting_process_id", "processor_name", "id"): _*
    )
    .register()

  val flowLatencyGauge = Gauge
    .build()
    .name("nifi_flow_latency")
    .help("measure of latency in a chain of processors in millisec")
    .labelNames(
      Array("process_group", "process_group_id", "starting_process", "starting_process_id", "processor_name", "id"): _*
    )
    .register();
  val backlogGauge = Gauge
    .build()
    .name("nifi_flow_backlog")
    .help("input folder's backlog in bytes and filecounts")
    .labelNames(
      Array("process_group", "process_group_id", "processor_name", "id", "type"): _*
    )
    .register()

  val congestionGuage = Gauge
    .build()
    .name("nifi_flow_congestion")
    .help("nifi congestion rate")
    .labelNames(
      Array("process_group", "process_group_id"): _*
    )
    .register()

  val flowCountGauge = Gauge
    .build()
    .name("nifi_flow_count")
    .help("number of flowfile in and out / byte over 5min")
    .labelNames(
      Array("process_group", "process_group_id", "starting_process", "starting_process_id", "processor_name", "id", "type"): _*
    )
    .register()

  def getHDFSFolderSummary(conf: Configuration, directory: String): (Long, Long) = {
    val fs   = FileSystem.get(conf)
    val path = new Path(directory)
    (fs.getContentSummary(path).getFileCount, fs.getContentSummary(path).getLength)
  }

  var token =""
  var url = ""
  def main(args: Array[String]): Unit = {

    val mainProcessGroupId = "ab32d7bd-0174-1000-6691-72922f8a7f91"
    url                = "http://localhost:8081"

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

    import ysj.nifi.JSONFunctions._
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

      val graph = NifiProcessorGraphBuilder.getGraph(mainProcessGroupId, url)
      traverseGraph(graph,exportProcessors)
      Thread.sleep(crawlInterval)
    }
  }

  def traverseGraph(graph: Map[String, NifiProcessor], processFlow: Array[NifiProcessor] => Unit): Unit ={
    var visitedProcessor = Map[(String, String), Int]()
    def traverse(p1: NifiProcessor,
                 source: String = "",
                 list: Array[NifiProcessor] = Array[NifiProcessor]()): Unit = {
      visitedProcessor = visitedProcessor + ((p1.id, source) -> 1)
      val chain = list ++ Array(p1)
      processFlow(chain)
      p1.processors.foreach(p2 => {
        if (visitedProcessor.get(p2.id, p1.id).getOrElse(0) != 1) {
          val chain = list ++ Array(p1)
          traverse(p2, p1.id,chain)
        }
      })
    }
    graph.foreach(x => {
      traverse(x._2)
      visitedProcessor = Map[(String, String), Int]()
    })
  }

  def exportProcessors(processorChain: Array[NifiProcessor]): Unit ={
    println(processorChain.map(_.name).mkString(","))
    processorChain.last.componentType match {

      case NifiProcessorType.FLOW_CONNECTION =>
        val group = processorChain.last.group
        /***********************/
        val congestGuage = congestionGuage.labels(group.get.name, group.get.id)
        val congestionRate = processorChain.last.status.get.percentUseBytes.get
        if(congestGuage.get()<congestionRate){
          congestGuage.set(congestionRate)
        }

      case NifiProcessorType.FLOW_PROCESS =>
        val flowIsRunning = processorChain.filter(_.componentType==NifiProcessorType.FLOW_PROCESS)
          .filterNot(p=>{
            p.status.isDefined && p.status.get.runStatus.getOrElse("").equalsIgnoreCase("running")
          }).length == 0
        val hasGroup = processorChain.head.group
        val groupName = if(hasGroup.isDefined) hasGroup.get.name else ""
        val groupId = if(hasGroup.isDefined) hasGroup.get.id else ""
        val status = processorChain.last.status
        val startingProcessor = processorChain.head
        val currentProcessor = processorChain.last
        val labels = Seq(groupName,
          groupId,
          startingProcessor.name,
          startingProcessor.id,
          currentProcessor.name,
          currentProcessor.id)
        /***********************/
        // export flow running state
        flowRunStateGauge.labels(
          labels :_*
        ).set(if(flowIsRunning) 1 else 0)

        /***********************/
        //export lineage duration
        val lineageDuration = NifiApiCaller.getLineageDuration(url, processorChain.last.id, token)
        if(lineageDuration.isDefined){
          flowLatencyGauge.labels(
            labels :_*
          ).set(lineageDuration.get)
        }

        /***********************/
        val flowfileIn   = currentProcessor.status.getOrElse(NifiProcessorStatus()).flowFilesIn
        if (flowfileIn.isDefined) {
          flowCountGauge
            .labels(
              (labels ++ Seq("flowfileIn")):_*
            )
            .set(flowfileIn.get)
        }
        /***********************/
        val flowFilesOut = currentProcessor.status.getOrElse(NifiProcessorStatus()).flowFilesOut
        if (flowFilesOut.isDefined) {
          flowCountGauge
            .labels(
              (labels ++ Seq("flowFilesOut")):_*
            )
            .set(flowFilesOut.get)
        }
        /***********************/
        val bytesRead    = currentProcessor.status.getOrElse(NifiProcessorStatus()).bytesRead
        if (bytesRead.isDefined) {
          flowCountGauge
            .labels(
              (labels ++ Seq("bytesRead")):_*
            )
            .set(bytesRead.get)
        }
        /***********************/
        val bytesWritten = currentProcessor.status.getOrElse(NifiProcessorStatus()).bytesWritten
        if (bytesWritten.isDefined) {
          flowCountGauge
            .labels(
              (labels ++ Seq("bytesWritten")):_*
            )
            .set(bytesWritten.get)
        }

        /***********************/
        if(currentProcessor.name.toUpperCase.contains("FETCH") && currentProcessor.name.toUpperCase.contains("HDFS")){
          val list_processors = processorChain
            .filter(processor => {
              processor.name.toUpperCase.contains("HDFS") && processor.name.toUpperCase.contains("LIST")
            })
          if(list_processors.length>0){
            val list_processor = list_processors(0)

            val str = NifiApiCaller.getProcess(url, list_processor.id, token)
            import ysj.nifi.JSONFunctions._
            val properties = new JSONParser()
              .parse(str)
              .asInstanceOf[JSONObject]
              .getJson("component")
              .getJson("config")
              .getJson("properties")
            val path = properties
              .get("Directory")
              .asInstanceOf[String]
            /******************
            println(path)
            getHDFSFolderSummary
            ******************/

          }
        }


      case _ =>
    }

  }
}
