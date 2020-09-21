package nifi
import scala.reflect.runtime.universe._
object NifiProcessorStatus {

  lazy val fields =
    classAccessors[NifiProcessorStatus].map(_.toString.split(" ")(1)).sortWith((x, y) => { x.compareTo(y) < 0 })

  def main(arg: Array[String]): Unit =
    println(fields.mkString(","))

  def getCCParams(cc: AnyRef) =
    cc.getClass.getDeclaredFields.foldLeft(Map.empty[String, Any]) { (a, f) =>
      f.setAccessible(true)
      a + (f.getName -> f.get(cc))
    }

  def classAccessors[T: TypeTag]: List[MethodSymbol] =
    typeOf[T].members.collect {
      case m: MethodSymbol if m.isCaseAccessor => m
    }.toList

  def createObject(values: Array[Option[Any]]): NifiProcessorStatus =
    createCaseClass[NifiProcessorStatus]((fields zip values).toMap)

  def createCaseClass[T](vals: Map[String, _ <: Object])(implicit cmf: ClassManifest[T]) = {
    val ctor = cmf.erasure.getConstructors().head
    val args = cmf.erasure.getDeclaredFields().map(f => vals(f.getName))
    ctor.newInstance(args: _*).asInstanceOf[T]
  }

}

case class NifiProcessorStatus(
    groupId: Option[String] = Option.empty[String],
    id: Option[String] = Option.empty[String],
    name: Option[String] = Option.empty[String],
    runStatus: Option[String] = Option.empty[String],
    statsLastRefreshed: Option[String] = Option.empty[String],
    sourceId: Option[String] = Option.empty[String],
    sourceName: Option[String] = Option.empty[String],
    destinationId: Option[String] = Option.empty[String],
    destinationName: Option[String] = Option.empty[String],
    aggregateSnapshot_id: Option[String] = Option.empty[String],
    aggregateSnapshot_groupId: Option[String] = Option.empty[String],
    aggregateSnapshot_name: Option[String] = Option.empty[String],
    aggregateSnapshot_type: Option[String] = Option.empty[String],
    aggregateSnapshot_runStatus: Option[String] = Option.empty[String],
    aggregateSnapshot_executionNode: Option[String] = Option.empty[String],
    aggregateSnapshot_bytesRead: Option[Long] = Option.empty[Long],
    aggregateSnapshot_bytesWritten: Option[Long] = Option.empty[Long],
    aggregateSnapshot_read: Option[String] = Option.empty[String],
    aggregateSnapshot_written: Option[String] = Option.empty[String],
    aggregateSnapshot_flowFilesIn: Option[Long] = Option.empty[Long],
    aggregateSnapshot_bytesIn: Option[Long] = Option.empty[Long],
    aggregateSnapshot_input: Option[String] = Option.empty[String],
    aggregateSnapshot_flowFilesOut: Option[Long] = Option.empty[Long],
    aggregateSnapshot_bytesOut: Option[Long] = Option.empty[Long],
    aggregateSnapshot_output: Option[String] = Option.empty[String],
    aggregateSnapshot_taskCount: Option[Long] = Option.empty[Long],
    aggregateSnapshot_tasksDurationNanos: Option[Long] = Option.empty[Long],
    aggregateSnapshot_tasks: Option[String] = Option.empty[String],
    aggregateSnapshot_tasksDuration: Option[String] = Option.empty[String],
    aggregateSnapshot_activeThreadCount: Option[Int] = Option.empty[Int],
    aggregateSnapshot_terminatedThreadCount: Option[Int] = Option.empty[Int],
    aggregateSnapshot_sourceName: Option[String] = Option.empty[String],
    aggregateSnapshot_destinationName: Option[String] = Option.empty[String],
    aggregateSnapshot_flowFilesQueued: Option[Long] = Option.empty[Long],
    aggregateSnapshot_bytesQueued: Option[Long] = Option.empty[Long],
    aggregateSnapshot_queued: Option[String] = Option.empty[String],
    aggregateSnapshot_queuedSize: Option[String] = Option.empty[String],
    aggregateSnapshot_queuedCount: Option[String] = Option.empty[String],
    aggregateSnapshot_percentUseCount: Option[Int] = Option.empty[Int],
    aggregateSnapshot_percentUseBytes: Option[Int] = Option.empty[Int],
    aggregateSnapshot_flowFilesTransferred: Option[String] = Option.empty[String],
    aggregateSnapshot_bytesTransferred: Option[String] = Option.empty[String],
    aggregateSnapshot_transferred: Option[String] = Option.empty[String],
    aggregateSnapshot_bytesReceived: Option[String] = Option.empty[String],
    aggregateSnapshot_flowFilesReceived: Option[String] = Option.empty[String],
    aggregateSnapshot_received: Option[String] = Option.empty[String],
    aggregateSnapshot_bytesSent: Option[String] = Option.empty[String],
    aggregateSnapshot_flowFilesSent: Option[String] = Option.empty[String],
    aggregateSnapshot_sent: Option[String] = Option.empty[String]
) {
  def printStatus(): Unit = {
    var status = this
    println("status.groupId: " + status.groupId)
    println("status.id: " + status.id)
    println("status.name: " + status.name)
    println("status.runStatus: " + status.runStatus)
    println("status.statsLastRefreshed: " + status.statsLastRefreshed)
    println("status.sourceId: " + status.sourceId)
    println("status.sourceName: " + status.sourceName)
    println("status.destinationId: " + status.destinationId)
    println("status.destinationName: " + status.destinationName)
    println("status.aggregateSnapshot_id: " + status.aggregateSnapshot_id)
    println("status.aggregateSnapshot_groupId: " + status.aggregateSnapshot_groupId)
    println("status.aggregateSnapshot_name: " + status.aggregateSnapshot_name)
    println("status.aggregateSnapshot_type: " + status.aggregateSnapshot_type)
    println("status.aggregateSnapshot_runStatus: " + status.aggregateSnapshot_runStatus)
    println("status.aggregateSnapshot_executionNode: " + status.aggregateSnapshot_executionNode)
    println("status.aggregateSnapshot_bytesRead: " + status.aggregateSnapshot_bytesRead)
    println("status.aggregateSnapshot_bytesWritten: " + status.aggregateSnapshot_bytesWritten)
    println("status.aggregateSnapshot_read: " + status.aggregateSnapshot_read)
    println("status.aggregateSnapshot_written: " + status.aggregateSnapshot_written)
    println("status.aggregateSnapshot_flowFilesIn: " + status.aggregateSnapshot_flowFilesIn)
    println("status.aggregateSnapshot_bytesIn: " + status.aggregateSnapshot_bytesIn)
    println("status.aggregateSnapshot_input: " + status.aggregateSnapshot_input)
    println("status.aggregateSnapshot_flowFilesOut: " + status.aggregateSnapshot_flowFilesOut)
    println("status.aggregateSnapshot_bytesOut: " + status.aggregateSnapshot_bytesOut)
    println("status.aggregateSnapshot_output: " + status.aggregateSnapshot_output)
    println("status.aggregateSnapshot_taskCount: " + status.aggregateSnapshot_taskCount)
    println("status.aggregateSnapshot_tasksDurationNanos: " + status.aggregateSnapshot_tasksDurationNanos)
    println("status.aggregateSnapshot_tasks: " + status.aggregateSnapshot_tasks)
    println("status.aggregateSnapshot_tasksDuration: " + status.aggregateSnapshot_tasksDuration)
    println("status.aggregateSnapshot_activeThreadCount: " + status.aggregateSnapshot_activeThreadCount)
    println("status.aggregateSnapshot_terminatedThreadCount: " + status.aggregateSnapshot_terminatedThreadCount)
    println("status.aggregateSnapshot_sourceName: " + status.aggregateSnapshot_sourceName)
    println("status.aggregateSnapshot_destinationName: " + status.aggregateSnapshot_destinationName)
    println("status.aggregateSnapshot_flowFilesQueued: " + status.aggregateSnapshot_flowFilesQueued)
    println("status.aggregateSnapshot_bytesQueued: " + status.aggregateSnapshot_bytesQueued)
    println("status.aggregateSnapshot_queued: " + status.aggregateSnapshot_queued)
    println("status.aggregateSnapshot_queuedSize: " + status.aggregateSnapshot_queuedSize)
    println("status.aggregateSnapshot_queuedCount: " + status.aggregateSnapshot_queuedCount)
    println("status.aggregateSnapshot_percentUseCount: " + status.aggregateSnapshot_percentUseCount)
    println("status.aggregateSnapshot_percentUseBytes: " + status.aggregateSnapshot_percentUseBytes)
    println("status.aggregateSnapshot_flowFilesTransferred: " + status.aggregateSnapshot_flowFilesTransferred)
    println("status.aggregateSnapshot_bytesTransferred: " + status.aggregateSnapshot_bytesTransferred)
    println("status.aggregateSnapshot_transferred: " + status.aggregateSnapshot_transferred)
    println("status.aggregateSnapshot_bytesReceived: " + status.aggregateSnapshot_bytesReceived)
    println("status.aggregateSnapshot_flowFilesReceived: " + status.aggregateSnapshot_flowFilesReceived)
    println("status.aggregateSnapshot_received: " + status.aggregateSnapshot_received)
    println("status.aggregateSnapshot_bytesSent: " + status.aggregateSnapshot_bytesSent)
    println("status.aggregateSnapshot_flowFilesSent: " + status.aggregateSnapshot_flowFilesSent)
    println("status.aggregateSnapshot_sent: " + status.aggregateSnapshot_sent)
    println("===================================================")
  }
}
