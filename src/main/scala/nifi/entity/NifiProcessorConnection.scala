package nifi.entity

case class NifiProcessorConnection(srcId: String,
                                   srcName: String,
                                   srcGroupId: String,
                                   connectionId: String,
                                   connectionName: String,
                                   dstId: String,
                                   dstName: String,
                                   dstGroupId: String,
                                   statuses: NifiProcessorStatus)
