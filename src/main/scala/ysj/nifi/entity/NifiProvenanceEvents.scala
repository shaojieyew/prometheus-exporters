package ysj.nifi.entity

case class NifiProvenanceEvents(
    id: Option[String] = Option.empty[String],
    eventId: Option[Long] = Option.empty[Long],
    eventTime: Option[String] = Option.empty[String],
    eventDuration: Option[Long] = Option.empty[Long],
    lineageDuration: Option[Long] = Option.empty[Long],
    eventType: Option[String] = Option.empty[String],
    flowFileUuid: Option[String] = Option.empty[String],
    fileSize: Option[String] = Option.empty[String],
    fileSizeBytes: Option[Long] = Option.empty[Long],
    groupId: Option[String] = Option.empty[String],
    componentId: Option[String] = Option.empty[String],
    componentType: Option[String] = Option.empty[String],
    componentName: Option[String] = Option.empty[String]
)
