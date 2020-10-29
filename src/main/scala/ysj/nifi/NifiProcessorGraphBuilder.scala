package ysj.nifi

import org.json.simple.parser.JSONParser
import org.json.simple.{JSONArray, JSONObject}
import ysj.nifi.JSONFunctions._
import ysj.nifi.NifiProcessorType._
import ysj.nifi.entity.{NifiProcessorConnection, NifiProcessorStatus}
object NifiProcessorType {
  val FLOW_PROCESS_GROUP = "processGroups"
  val FLOW_PROCESS       = "processors"
  val FLOW_CONNECTION    = "connections"
  val FLOW_OUTPUTPORT    = "outputPorts"
  val FLOW_INPUTPORT     = "inputPorts"
}

object NifiProcessorGraphBuilder {
  class NifiProcessor(var id: String,
                      var name: String,
                      var componentType: String,
                      var status: Option[NifiProcessorStatus] = Option.empty[NifiProcessorStatus],
                      var group: Option[NifiProcessor] = Option.empty[NifiProcessor],
                      var processors: Array[NifiProcessor] = Array[NifiProcessor](),
                      var srcCount: Int = 0,
                      var dstCount: Int = 0)

  lazy val NEW_GRAPH = Map[String, NifiProcessor]()
  var host           = ""
  var token          = ""

  def main(args: Array[String]): Unit = {
    val mainProcessGroup = "ab32d7bd-0174-1000-6691-72922f8a7f91"
    val graph            = NifiProcessorGraphBuilder.getGraph(mainProcessGroup, "http://localhost:8081")
    graph.printProcessGroupFlow()
    val flattenedGraph = graph.flattenGraph
    flattenedGraph.printFlattenedFlow()
  }

  def getGraph(id: String, host: String, token: String = ""): Map[String, NifiProcessor] = {
    this.host = host
    this.token = token
    NEW_GRAPH.buildProcessGroupFlow(id).filter(r => r._2.srcCount == 0)
  }

  implicit class NifiProcessorGraphFuntions(var graph: Map[String, NifiProcessor]) {

    def flattenGraph(): Array[Array[NifiProcessor]] = {
      var flattenedGraph   = Array[Array[NifiProcessor]]()
      var visitedProcessor = Map[(String, String), Int]()

      def traverse(p1: NifiProcessor,
                   source: String = "",
                   list: Array[NifiProcessor] = Array[NifiProcessor]()): Unit = {
        visitedProcessor = visitedProcessor + ((p1.id, source) -> 1)
        p1.processors.foreach(p2 => {
          if (visitedProcessor.get(p2.id, p1.id).getOrElse(0) != 1) {
            traverse(p2, p1.id, list ++ Array(p1))
          } else {
            flattenedGraph = flattenedGraph ++ Array(list ++ Array(p1, p2))
          }
        })
        if (p1.processors.length == 0) {
          flattenedGraph = flattenedGraph ++ Array(list ++ Array(p1))
        }
      }
      graph.foreach(x => {
        traverse(x._2)
        visitedProcessor = Map[(String, String), Int]()
      })
      flattenedGraph
    }

    def addStatuses(status: NifiProcessorStatus,
                    componentType: String,
                    mainProcessor: Option[NifiProcessor] = Option.empty[NifiProcessor]): Option[NifiProcessor] =
      status.id match {
        case Some(id) =>
          graph.get(status.id.get) match {
            case Some(processor) =>
              processor.status = Option(status)
              Option(processor)

            case None =>
              val newProcessor = new NifiProcessor(status.id.get,
                                                   status.name.get,
                                                   componentType,
                                                   group = mainProcessor,
                                                   status = Option(status))
              graph = graph + (id -> newProcessor)
              Option(newProcessor)
          }
      }

    val kerberosEnabled = false

    def buildProcessGroupFlow(
        id: String,
        groupProcessor: Option[NifiProcessor] = Option.empty[NifiProcessor]
    ): Map[String, NifiProcessor] = {
      val stringResponse = NifiApiCaller.getProcessGroup(host, id, token)
      val json           = new JSONParser().parse(stringResponse).asInstanceOf[JSONObject]

      val processGroupFlow = json
        .getJson("processGroupFlow")
        .getJson("flow")

      val name = json
        .getJson("processGroupFlow")
        .getJson("breadcrumb")
        .getJson("breadcrumb")
        .get("name")
        .asInstanceOf[String]

      val parentProcessor = groupProcessor match {
        case None             => new NifiProcessor(id, name, FLOW_PROCESS_GROUP)
        case Some(gProcessor) => gProcessor
      }

      if (processGroupFlow != null) {
        getFlowComponentBytype(processGroupFlow, FLOW_PROCESS_GROUP)
          .map(
            gprocessor => {
              gprocessor
                .getJson("status")
                .processStatusParser
            }
          )
          .foreach(status => {
            val newProcessor = addStatuses(status, FLOW_PROCESS_GROUP, Option(parentProcessor))
            status.id match {
              case Some(id) => graph = graph.buildProcessGroupFlow(id, newProcessor)
            }
          })

        Seq(FLOW_PROCESS, FLOW_INPUTPORT, FLOW_OUTPUTPORT).foreach(componentType => {
          getFlowComponentBytype(processGroupFlow, componentType)
            .map(
              processor => {
                processor
                  .getJson("status")
                  .processStatusParser
              }
            )
            .foreach(
              status => {
                addStatuses(status, FLOW_PROCESS, Option(parentProcessor))
              }
            )

        })

        getFlowComponentBytype(processGroupFlow, FLOW_CONNECTION)
          .map(connection => {
            NifiProcessorConnection(
              connection.getFieldByPath("component.source.id").asInstanceOf[String],
              connection.getFieldByPath("component.source.name").asInstanceOf[String],
              connection.getFieldByPath("component.source.groupId").asInstanceOf[String],
              connection.getFieldByPath("status.id").asInstanceOf[String],
              connection.getFieldByPath("status.name").asInstanceOf[String],
              connection.getFieldByPath("component.destination.id").asInstanceOf[String],
              connection.getFieldByPath("component.destination.name").asInstanceOf[String],
              connection.getFieldByPath("component.destination.groupId").asInstanceOf[String],
              connection.get("status").asInstanceOf[JSONObject].processStatusParser()
            )
          })
          .foreach(connection => {
            var srcGroup = Option.empty[NifiProcessor]
            var dstGroup = Option.empty[NifiProcessor]
            if (parentProcessor.id == connection.srcGroupId) {
              srcGroup = Option(parentProcessor)
            }
            if (parentProcessor.id == connection.dstGroupId) {
              dstGroup = Option(parentProcessor)
            }
            graph = graph
              .addConnectionsToMap(connection.srcId,
                                   connection.srcName,
                                   srcGroup,
                                   FLOW_PROCESS,
                                   connection.connectionId,
                                   connection.connectionName,
                                   dstGroup,
                                   FLOW_CONNECTION)
              .addConnectionsToMap(connection.connectionId,
                                   connection.connectionName,
                                   dstGroup,
                                   FLOW_CONNECTION,
                                   connection.dstId,
                                   connection.dstName,
                                   dstGroup,
                                   FLOW_PROCESS)

            addStatuses(connection.statuses, FLOW_CONNECTION, dstGroup)
          })
      }
      graph
    }

    def printProcessGroupFlow(): Unit = {
      var visitedProcessor = Map[(String, String), Int]()
      def printProcessor(p1: NifiProcessor, source: String = "", padding: String = ""): Unit = {
        if (p1.componentType == FLOW_CONNECTION) { print(padding + p1.name + " ==>") } else {
          p1.group match {
            case Some(_) => println(p1.group.get.name + ":" + p1.name)
            case None    => println(p1.name)
          }
        }

        visitedProcessor = visitedProcessor + ((p1.id, source) -> 1)
        p1.processors.foreach(p2 => {
          if (visitedProcessor.get(p2.id, p1.id).getOrElse(0) != 1) {
            printProcessor(p2, p1.id, padding + "\t")
          } else {
            p2.group match {
              case Some(_) => println("*" + p2.group.get.name + ":" + p2.name)
              case None    => println("*" + p2.name)
            }
          }
        })
      }
      graph.foreach(x => {
        printProcessor(x._2)
        visitedProcessor = Map[(String, String), Int]()
      })
    }

    def addConnectionsToMap(srcId: String,
                            srcName: String,
                            srcGroup: Option[NifiProcessor],
                            srcComponentType: String,
                            dstId: String,
                            dstName: String,
                            dstGroup: Option[NifiProcessor],
                            dstComponentType: String): Map[String, NifiProcessor] = {
      var srcProcessor = graph.get(srcId).getOrElse(null)
      if (srcProcessor == null) {
        srcProcessor = new NifiProcessor(srcId, srcName, srcComponentType, group = srcGroup)
      }
      srcProcessor.dstCount += 1
      srcProcessor.processors = srcProcessor.processors ++ Array(
        {
          val dstProcessor = graph.get(dstId) match {
            case Some(processor) => processor
            case None =>
              val processor = new NifiProcessor(dstId, dstName, dstComponentType, group = dstGroup)
              graph = graph + (dstId -> processor)
              processor

          }
          dstProcessor.srcCount += 1
          dstProcessor
        }
      )
      graph = graph + (srcId -> srcProcessor)
      graph
    }

    private def getFlowComponentBytype(processflow: JSONObject, flowType: String): Array[JSONObject] = {
      var processArray = Array[JSONObject]()
      val processes    = processflow.get(flowType).asInstanceOf[JSONArray]

      for (i <- 0 to processes.size() - 1) {
        val process =
          processes.get(i).asInstanceOf[JSONObject]
        processArray = processArray ++ Array(process)
      }
      processArray
    }
  }

  implicit class NifiProcessorFlattenedGraphFuntions(flattenedGraph: Array[Array[NifiProcessor]]) {
    def printFlattenedFlow(): Unit =
      flattenedGraph.foreach(processorChain => {
        processorChain
          .map(record => {
            record.group match {
              case Some(_) => record.group.get.name + ":" + record.name
              case None    => record.name
            }
          })
          .mkString("-")
          .foreach(print)
        println("")
      })
  }

}
