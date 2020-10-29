package ysj.nifi.entity

object NifiProcessorStatus {
  def getFields(): Array[String] =
    NifiProcessorStatus.getClass.getDeclaredFields.map(_.getName).sortWith((x, y) => { x.compareTo(y) < 0 })

  def main(arg: Array[String]): Unit =
    println(getFields().mkString(","))

}

case class NifiProcessorStatus11(
    groupId: Option[String] = Option.empty[String],
    id: Option[String] = Option.empty[String],
    name: Option[String] = Option.empty[String],
    runStatus: Option[String] = Option.empty[String],
    statsLastRefreshed: Option[String] = Option.empty[String],
    sourceId: Option[String] = Option.empty[String],
    sourceName: Option[String] = Option.empty[String],
    destinationId: Option[String] = Option.empty[String],
    destinationName: Option[String] = Option.empty[String],
    componentType: Option[String] = Option.empty[String],
    executionNode: Option[String] = Option.empty[String],
    bytesRead: Option[Long] = Option.empty[Long],
    bytesWritten: Option[Long] = Option.empty[Long],
    read: Option[String] = Option.empty[String],
    written: Option[String] = Option.empty[String],
    flowFilesIn: Option[Long] = Option.empty[Long],
    bytesIn: Option[Long] = Option.empty[Long],
    input: Option[String] = Option.empty[String],
    flowFilesOut: Option[Long] = Option.empty[Long],
    bytesOut: Option[Long] = Option.empty[Long],
    output: Option[String] = Option.empty[String],
    taskCount: Option[Long] = Option.empty[Long],
    tasksDurationNanos: Option[Long] = Option.empty[Long],
    tasks: Option[String] = Option.empty[String],
    tasksDuration: Option[String] = Option.empty[String],
    activeThreadCount: Option[Long] = Option.empty[Long],
    terminatedThreadCount: Option[Long] = Option.empty[Long],
    flowFilesQueued: Option[Long] = Option.empty[Long],
    bytesQueued: Option[Long] = Option.empty[Long],
    queued: Option[String] = Option.empty[String],
    queuedSize: Option[String] = Option.empty[String],
    queuedCount: Option[String] = Option.empty[String],
    percentUseCount: Option[Long] = Option.empty[Long],
    percentUseBytes: Option[Long] = Option.empty[Long],
    flowFilesTransferred: Option[Long] = Option.empty[Long],
    bytesTransferred: Option[Long] = Option.empty[Long],
    transferred: Option[String] = Option.empty[String],
    bytesReceived: Option[Long] = Option.empty[Long],
    flowFilesReceived: Option[Long] = Option.empty[Long],
    received: Option[String] = Option.empty[String],
    bytesSent: Option[Long] = Option.empty[Long],
    flowFilesSent: Option[Long] = Option.empty[Long],
    sent: Option[String] = Option.empty[String]
)
