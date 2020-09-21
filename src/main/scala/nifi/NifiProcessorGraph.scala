package nifi

import io.circe.{ parser, ACursor, HCursor, Json }
import nifi.NifiProcessorEndpoint.PROCESS_GROUPS_URL
import nifi.NifiProcessorGraph.host
import nifi.NifiProcessorType.{ FLOW_CONNECTION, FLOW_INPUTPORT, FLOW_OUTPUTPORT, FLOW_PROCESS, FLOW_PROCESS_GROUP }
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClientBuilder

object NifiProcessorType {
  val FLOW_PROCESS_GROUP = "processGroups"
  val FLOW_PROCESS       = "processors"
  val FLOW_CONNECTION    = "connections"
  val FLOW_OUTPUTPORT    = "outputPorts"
  val FLOW_INPUTPORT     = "inputPorts"
}
object NifiProcessorEndpoint {
  val PROCESS_GROUPS_URL = "%s/nifi-api/flow/process-groups/%s"
}

object NifiProcessorGraph {
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

  def main(args: Array[String]): Unit = {
    val mainProcessGroup = "ab32d7bd-0174-1000-6691-72922f8a7f91"
    val graph            = NifiProcessorGraph.getGraph(mainProcessGroup, "http://localhost:8081")
    val flattenedGraph   = graph.flattenGraph
    //flattenedGraph.filter(_(0).id == "aef82a13-0174-1000-39b7-7ee94c420ff6").printFlattenedFlow()
    /*
    flattenedGraph
      .filter(_(0).id == "afe7792e-0174-1000-a071-8586d3dfae80")(0)
      .filter(_.status.isDefined)
      .foreach(r => {
        r.status.get.printStatus()
      })
   */
  }

  def getGraph(id: String, host: String): Map[String, NifiProcessor] = {
    this.host = host
    NEW_GRAPH.buildProcessGroupFlow(id).filter(r => r._2.srcCount == 0)
  }

  implicit class NifiProcessorGraphFuntions(var graph: Map[String, NifiProcessor]) {

    def flattenGraph(): Array[Array[NifiProcessor]] = {
      var flattenedGraph   = Array[Array[NifiProcessor]]()
      var visitedProcessor = Map[String, Int]()

      def traverse(p1: NifiProcessor, list: Array[NifiProcessor] = Array[NifiProcessor]()): Unit = {
        visitedProcessor = visitedProcessor + (p1.id -> 1)
        p1.processors.foreach(p2 => {
          if (visitedProcessor.get(p2.id).getOrElse(0) != 1) {
            traverse(p2, list ++ Array(p1))
          } else {
            flattenedGraph = flattenedGraph ++ Array(list ++ Array(p1, p2))
          }
        })
        if (p1.processors.length == 0) {
          flattenedGraph = flattenedGraph ++ Array(list ++ Array(p1))
        }
      }
      graph.foreach(x => traverse(x._2))
      flattenedGraph
    }

    def addStatuses(status: NifiProcessorStatus,
                    componentType: String,
                    mainProcessor: Option[NifiProcessor] = Option.empty[NifiProcessor]): Option[NifiProcessor] =
      status.id match {
        case Some(id) => {
          graph.get(status.id.get) match {
            case Some(processor) => {
              processor.status = Option(status)
              Option(processor)
            }
            case None => {
              val newProcessor = new NifiProcessor(status.id.get,
                                                   status.name.get,
                                                   componentType,
                                                   group = mainProcessor,
                                                   status = Option(status))
              graph = graph + (id -> newProcessor)
              Option(newProcessor)
            }
          }
        }
      }

    def buildProcessGroupFlow(
        id: String,
        groupProcessor: Option[NifiProcessor] = Option.empty[NifiProcessor]
    ): Map[String, NifiProcessor] = {
      val response    = HttpClientBuilder.create.build().execute(new HttpGet(PROCESS_GROUPS_URL.format(host, id)))
      val inputStream = response.getEntity.getContent
      val result      = new String(IOUtils.toByteArray(inputStream), "UTF-8")

      val json: Json = parser.parse(result).right.getOrElse(Json.Null)

      val processGroupFlow = json.hcursor
        .downField("processGroupFlow")
        .downField("flow")
        .focus

      val name = json.hcursor
        .downField("processGroupFlow")
        .downField("breadcrumb")
        .downField("breadcrumb")
        .get[String]("name")
        .right
        .getOrElse("")

      val parentProcessor = groupProcessor match {
        case None             => { new NifiProcessor(id, name, FLOW_PROCESS_GROUP) }
        case Some(gProcessor) => { gProcessor }
      }

      val statusFields = NifiProcessorStatus.fields.map(_.replace("_", ".")).toArray[String]
      if (processGroupFlow.isDefined) {
        getFlowComponentBytype(processGroupFlow.get, FLOW_PROCESS_GROUP)
          .map(gprocessor => gprocessor.hcursor)
          .map(
            gprocessor => {
              gprocessor
                .downField("status")
                .getFieldsByPaths(statusFields)
            }
          )
          .map(_.map(Option(_)))
          .map(NifiProcessorStatus.createObject(_))
          .foreach(status => {
            val newProcessor = addStatuses(status, FLOW_PROCESS_GROUP, Option(parentProcessor))
            status.id match {
              case Some(id) => { graph = graph.buildProcessGroupFlow(id, newProcessor) }
            }
          })

        Seq(FLOW_PROCESS, FLOW_INPUTPORT, FLOW_OUTPUTPORT).foreach(componentType => {

          getFlowComponentBytype(processGroupFlow.get, componentType)
            .map(processor => processor.hcursor)
            .map(
              processor => {
                processor
                  .downField("status")
                  .getFieldsByPaths(statusFields)
              }
            )
            .map(_.map(Option(_)))
            .map(NifiProcessorStatus.createObject(_))
            .foreach(status => addStatuses(status, FLOW_PROCESS, Option(parentProcessor)))

        })

        getFlowComponentBytype(processGroupFlow.get, FLOW_CONNECTION)
          .map(connection => connection.hcursor)
          .map(connection => {
            val statusValue = connection.downField("status").getFieldsByPaths(statusFields).map(Option(_))
            (
              connection.getFieldByPath("component.source.id").asInstanceOf[String],
              connection.getFieldByPath("component.source.name").asInstanceOf[String],
              connection.getFieldByPath("component.source.groupId").asInstanceOf[String],
              connection.getFieldByPath("status.id").asInstanceOf[String],
              connection.getFieldByPath("status.name").asInstanceOf[String],
              connection.getFieldByPath("component.destination.id").asInstanceOf[String],
              connection.getFieldByPath("component.destination.name").asInstanceOf[String],
              connection.getFieldByPath("component.destination.groupId").asInstanceOf[String],
              NifiProcessorStatus
                .createObject(statusValue)
            )
          })
          .foreach(connectionRecord => {
            val (srcId, srcName, srcGroupId, connectionId, connectionName, dstId, dstName, dstGroupId, statuses) =
              connectionRecord
            var srcGroup = Option.empty[NifiProcessor]
            var dstGroup = Option.empty[NifiProcessor]
            if (parentProcessor.id == srcGroupId) {
              srcGroup = Option(parentProcessor)
            }
            if (parentProcessor.id == dstGroupId) {
              dstGroup = Option(parentProcessor)
            }
            graph = graph
              .addConnectionsToMap(srcId,
                                   srcName,
                                   srcGroup,
                                   FLOW_PROCESS,
                                   connectionId,
                                   connectionName,
                                   dstGroup,
                                   FLOW_CONNECTION)
              .addConnectionsToMap(connectionId,
                                   connectionName,
                                   dstGroup,
                                   FLOW_CONNECTION,
                                   dstId,
                                   dstName,
                                   dstGroup,
                                   FLOW_PROCESS)

            addStatuses(statuses, FLOW_CONNECTION, dstGroup)
          })
      }
      graph
    }

    def printProcessGroupFlow(): Unit = {
      var visitedProcessor = Map[String, Int]()
      def printProcessor(p1: NifiProcessor, padding: String): Unit = {
        if (p1.componentType == FLOW_CONNECTION) { print(padding + p1.name + " ==>") } else {
          p1.group match {
            case Some(_) => { println(p1.group.get.name + ":" + p1.name) }
            case None    => { println(p1.name) }
          }
        }

        visitedProcessor = visitedProcessor + (p1.id -> 1)
        p1.processors.foreach(p2 => {
          if (visitedProcessor.get(p2.id).getOrElse(0) != 1) {
            printProcessor(p2, padding + "\t")
          } else {
            p2.group match {
              case Some(_) => { println("*" + p2.group.get.name + ":" + p2.name) }
              case None    => { println("*" + p2.name) }
            }
          }
        })
      }
      graph.foreach(x => printProcessor(x._2, ""))
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
            case Some(processor) => { processor }
            case None => {
              val processor = new NifiProcessor(dstId, dstName, dstComponentType, group = dstGroup)
              graph = graph + (dstId -> processor)
              processor
            }
          }
          dstProcessor.srcCount += 1
          dstProcessor
        }
      )
      graph = graph + (srcId -> srcProcessor)
      graph
    }

    private def getFlowComponentBytype(processflow: Json, flowType: String): Array[Json] = {
      var processArray = Array[Json]()
      val processes = processflow.hcursor
        .downField(flowType)
      var i = 0
      while (processes.downN(i).focus.isDefined) {
        val process =
          processes
            .downN(i)
            .focus
            .get
        processArray = processArray ++ Array(process)
        i += 1
      }
      processArray
    }
  }
  implicit class ACursorFunctions(acursor: ACursor) {
    def getFieldsByPaths(fields: Array[String]): Array[Any] =
      fields.map(
        field => acursor.getFieldByPath(field)
      )
    def getFieldByPath(field: String): Any = {
      var getField   = field
      var subAcursor = acursor
      if (field.contains('.')) {
        val fieldArr = field
          .split('.')
        fieldArr
          .dropRight(1)
          .foreach(x => {
            subAcursor = subAcursor.downField(x)
          })
        getField = fieldArr.last
      }
      val strVal  = subAcursor.get[String](getField).right.getOrElse("")
      val longVal = subAcursor.get[Long](getField).right.getOrElse(-1)
      if (strVal.length == 0 && longVal != -1) {
        longVal
      } else {
        strVal
      }
    }
  }
  implicit class NifiProcessorFlattenedGraphFuntions(flattenedGraph: Array[Array[NifiProcessor]]) {
    def printFlattenedFlow(): Unit =
      flattenedGraph.foreach(processorChain => {
        processorChain
          .map(record => {
            record.group match {
              case Some(_) => { record.group.get.name + ":" + record.name }
              case None    => { record.name }
            }
          })
          .mkString("-")
          .foreach(print)
        println("")
      })
  }
}
