package nifi

import nifi.entity.{ NifiClusterSummary, NifiProcessorStatus, NifiProvenanceEvents }
import org.json.simple.{ JSONArray, JSONObject }
object JSONFunctions {
  implicit class NifiJsonParsingFunctions(json: JSONObject) {

    def getJson(field: String): JSONObject =
      json.get(field).asInstanceOf[JSONObject]

    def getJsonArray(field: String): JSONArray =
      json.get(field).asInstanceOf[JSONArray]

    def getJson(fields: Array[String]): JSONObject =
      if (fields.length == 1) {
        getJson(fields(0))
      } else {
        getJson(fields(0)).getJson(fields.drop(1))
      }
    def getFieldsByPaths(fields: Array[String]): Array[Any] =
      fields.map(
        field => json.getFieldByPath(field)
      )
    def getFieldByPath(field: String): Any = {
      var getField   = field
      var subAcursor = json
      if (field.contains('.')) {
        val fieldArr = field
          .split('.')
        fieldArr
          .dropRight(1)
          .foreach(x => {
            subAcursor = subAcursor.get(x).asInstanceOf[JSONObject]
          })
        getField = fieldArr.last
      }
      subAcursor.get(getField)
    }

    def clusterSummaryParser(): NifiClusterSummary =
      NifiClusterSummary(
        json
          .get("connectedNodeCount")
          .asInstanceOf[Long],
        json
          .get("totalNodeCount")
          .asInstanceOf[Long],
        json
          .get("clustered")
          .asInstanceOf[Boolean]
      )

    def provenanceParser(): NifiProvenanceEvents =
      NifiProvenanceEvents(
        id = Option(json.getFieldByPath("id").asInstanceOf[String]),
        eventId = Option(json.getFieldByPath("eventId").asInstanceOf[Long]),
        eventTime = Option(json.getFieldByPath("eventTime").asInstanceOf[String]),
        eventDuration = Option(json.getFieldByPath("eventDuration").asInstanceOf[Long]),
        lineageDuration = Option(json.getFieldByPath("lineageDuration").asInstanceOf[Long]),
        eventType = Option(json.getFieldByPath("eventType").asInstanceOf[String]),
        flowFileUuid = Option(json.getFieldByPath("flowFileUuid").asInstanceOf[String]),
        fileSize = Option(json.getFieldByPath("fileSize").asInstanceOf[String]),
        fileSizeBytes = Option(json.getFieldByPath("fileSizeBytes").asInstanceOf[Long]),
        groupId = Option(json.getFieldByPath("groupId").asInstanceOf[String]),
        componentId = Option(json.getFieldByPath("componentId").asInstanceOf[String]),
        componentType = Option(json.getFieldByPath("componentType").asInstanceOf[String]),
        componentName = Option(json.getFieldByPath("componentName").asInstanceOf[String])
      )

    def processStatusParser(): NifiProcessorStatus =
      NifiProcessorStatus(
        id = Option(json.getFieldByPath("id").asInstanceOf[String]),
        groupId = Option(json.getFieldByPath("groupId").asInstanceOf[String]),
        name = Option(json.getFieldByPath("name").asInstanceOf[String]),
        runStatus = Option(json.getFieldByPath("runStatus").asInstanceOf[String]),
        statsLastRefreshed = Option(json.getFieldByPath("statsLastRefreshed").asInstanceOf[String]),
        sourceId = Option(json.getFieldByPath("aggregateSnapshot.sourceId").asInstanceOf[String]),
        sourceName = Option(json.getFieldByPath("aggregateSnapshot.sourceName").asInstanceOf[String]),
        destinationId = Option(json.getFieldByPath("aggregateSnapshot.destinationId").asInstanceOf[String]),
        destinationName = Option(json.getFieldByPath("aggregateSnapshot.destinationName").asInstanceOf[String]),
        componentType = Option(json.getFieldByPath("aggregateSnapshot.componentType").asInstanceOf[String]),
        executionNode = Option(json.getFieldByPath("aggregateSnapshot.executionNode").asInstanceOf[String]),
        bytesRead = Option(json.getFieldByPath("aggregateSnapshot.bytesRead").asInstanceOf[Long]),
        bytesWritten = Option(json.getFieldByPath("aggregateSnapshot.bytesWritten").asInstanceOf[Long]),
        read = Option(json.getFieldByPath("aggregateSnapshot.read").asInstanceOf[String]),
        written = Option(json.getFieldByPath("aggregateSnapshot.written").asInstanceOf[String]),
        flowFilesIn = Option(json.getFieldByPath("aggregateSnapshot.flowFilesIn").asInstanceOf[Long]),
        bytesIn = Option(json.getFieldByPath("aggregateSnapshot.bytesIn").asInstanceOf[Long]),
        input = Option(json.getFieldByPath("aggregateSnapshot.input").asInstanceOf[String]),
        flowFilesOut = Option(json.getFieldByPath("aggregateSnapshot.flowFilesOut").asInstanceOf[Long]),
        bytesOut = Option(json.getFieldByPath("aggregateSnapshot.bytesOut").asInstanceOf[Long]),
        output = Option(json.getFieldByPath("aggregateSnapshot.output").asInstanceOf[String]),
        taskCount = Option(json.getFieldByPath("aggregateSnapshot.taskCount").asInstanceOf[Long]),
        tasksDurationNanos = Option(json.getFieldByPath("aggregateSnapshot.tasksDurationNanos").asInstanceOf[Long]),
        tasks = Option(json.getFieldByPath("aggregateSnapshot.tasks").asInstanceOf[String]),
        tasksDuration = Option(json.getFieldByPath("aggregateSnapshot.tasksDuration").asInstanceOf[String]),
        activeThreadCount = Option(json.getFieldByPath("aggregateSnapshot.activeThreadCount").asInstanceOf[Long]),
        terminatedThreadCount =
          Option(json.getFieldByPath("aggregateSnapshot.terminatedThreadCount").asInstanceOf[Long]),
        flowFilesQueued = Option(json.getFieldByPath("aggregateSnapshot.flowFilesQueued").asInstanceOf[Long]),
        bytesQueued = Option(json.getFieldByPath("aggregateSnapshot.bytesQueued").asInstanceOf[Long]),
        queued = Option(json.getFieldByPath("aggregateSnapshot.queued").asInstanceOf[String]),
        queuedSize = Option(json.getFieldByPath("aggregateSnapshot.queuedSize").asInstanceOf[String]),
        queuedCount = Option(json.getFieldByPath("aggregateSnapshot.queuedCount").asInstanceOf[String]),
        percentUseCount = Option(json.getFieldByPath("aggregateSnapshot.percentUseCount").asInstanceOf[Long]),
        percentUseBytes = Option(json.getFieldByPath("aggregateSnapshot.percentUseBytes").asInstanceOf[Long]),
        flowFilesTransferred = Option(json.getFieldByPath("aggregateSnapshot.flowFilesTransferred").asInstanceOf[Long]),
        bytesTransferred = Option(json.getFieldByPath("aggregateSnapshot.bytesTransferred").asInstanceOf[Long]),
        transferred = Option(json.getFieldByPath("aggregateSnapshot.transferred").asInstanceOf[String]),
        bytesReceived = Option(json.getFieldByPath("aggregateSnapshot.bytesReceived").asInstanceOf[Long]),
        flowFilesReceived = Option(json.getFieldByPath("aggregateSnapshot.flowFilesReceived").asInstanceOf[Long]),
        received = Option(json.getFieldByPath("aggregateSnapshot.received").asInstanceOf[String]),
        bytesSent = Option(json.getFieldByPath("aggregateSnapshot.bytesSent").asInstanceOf[Long]),
        flowFilesSent = Option(json.getFieldByPath("aggregateSnapshot.flowFilesSent").asInstanceOf[Long]),
        sent = Option(json.getFieldByPath("aggregateSnapshot.sent").asInstanceOf[String])
      )
  }
}
