package akka.persistence.cassandra.snapshot

import java.lang.{Long => JLong}
import java.nio.ByteBuffer

import akka.actor._
import akka.pattern.pipe
import akka.persistence._
import akka.persistence.cassandra._
import akka.persistence.serialization.Snapshot
import akka.serialization.SerializationExtension
import com.datastax.driver.core._
import com.datastax.driver.core.policies.{ExponentialReconnectionPolicy, Policies, TokenAwarePolicy}
import org.apache.cassandra.utils.ByteBufferUtil

import scala.collection.immutable
import scala.concurrent.Future
import scala.util._

/**
 * Optimized and fully async version of [[akka.persistence.snapshot.SnapshotStore]].
 */
trait CassandraSnapshotStoreEndpoint extends Actor {
  import SnapshotProtocol._
  import context.dispatcher

  val extension = Persistence(context.system)
  val publish = extension.settings.internal.publishPluginCommands

  def receive = {
    case LoadSnapshot(processorId, criteria, toSequenceNr) =>
      val p = sender
      loadAsync(processorId, criteria.limit(toSequenceNr)) map {
      
        sso => LoadSnapshotResult(sso, toSequenceNr)
      } recover {
        case e => LoadSnapshotResult(None, toSequenceNr)
      } pipeTo (p)
    case SaveSnapshot(metadata, snapshot) =>
      val p = sender
      val md = metadata.copy(timestamp = System.currentTimeMillis)
      saveAsync(md, snapshot) map {
        _ => SaveSnapshotSuccess(md)
      } recover {
        case e => SaveSnapshotFailure(metadata, e)
      } pipeTo (p)
    case d @ DeleteSnapshot(metadata) =>
      deleteAsync(metadata) onComplete {
        case Success(_) => if (publish) context.system.eventStream.publish(d)
        case Failure(_) =>
      }
    case d @ DeleteSnapshots(processorId, criteria) =>
      deleteAsync(processorId, criteria) onComplete {
        case Success(_) => if (publish) context.system.eventStream.publish(d)
        case Failure(_) =>
      }
  }

  def loadAsync(processorId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]]
  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit]
  def deleteAsync(metadata: SnapshotMetadata): Future[Unit]
  def deleteAsync(processorId: String, criteria: SnapshotSelectionCriteria): Future[Unit]
}

class CassandraSnapshotStore extends CassandraSnapshotStoreEndpoint with CassandraStatements with ActorLogging with CassandraPlugin {
  val config = new CassandraSnapshotStoreConfig(context.system.settings.config.getConfig("cassandra-snapshot-store"))
  val serialization = SerializationExtension(context.system)
  val logger = log

  import config._
  import context.dispatcher

val preparedDeleteSnapshotBatch : StringBuilder = new StringBuilder
   preparedDeleteSnapshotBatch.append("BEGIN BATCH")

  //load all those as lazy to no get PostRestartException in case C* does down, and at restart fails to init
  var cluster: Cluster = null
  var session: Session = null

  var preparedWriteSnapshot: PreparedStatement = null
  var preparedDeleteSnapshot: PreparedStatement = null
  var preparedSelectSnapshot: PreparedStatement = null
  var preparedSelectSnapshotMetadataForLoad: PreparedStatement = null
  var preparedSelectSnapshotMetadataForDelete: PreparedStatement = null
  var initialized = false

  override def preStart(): Unit = {
    super.preStart
    // eager initialization, but not from constructor
    self ! "init"
  }

  override def receive = ({
    case "init" => initializeCassandra()
  }: Receive) orElse super.receive

  def initializeCassandra(): Unit = {
    if (initialized) {
      log.warning("##### Desired to initialize a Cassandra snapshot that is already initialized ... - skip")
      return
    }
    log.info("##### Cassandra snapshot desire to lazy initialize ...")

    try {
      //try reconnect at max 1 minute, with token aware and round robin
      cluster = clusterBuilder
        .withReconnectionPolicy(new ExponentialReconnectionPolicy(1000, 1 * 60 * 1000))
        .withLoadBalancingPolicy(new TokenAwarePolicy(Policies.defaultLoadBalancingPolicy))
        .build()
      session = cluster.connect()
      createKeyspace(session)
      createTable(session, createTable)

      preparedWriteSnapshot = session.prepare(writeSnapshot).setConsistencyLevel(writeConsistency)
      preparedDeleteSnapshot = session.prepare(deleteSnapshot).setConsistencyLevel(writeConsistency)
      preparedSelectSnapshot = session.prepare(selectSnapshot).setConsistencyLevel(readConsistency)
      preparedSelectSnapshotMetadataForLoad = session.prepare(selectSnapshotMetadata(limit = Some(maxMetadataResultSize))).setConsistencyLevel(readConsistency)
      preparedSelectSnapshotMetadataForDelete = session.prepare(selectSnapshotMetadata(limit = None)).setConsistencyLevel(readConsistency)
      
    } catch {
      case e: Throwable =>
        initialized = false
        log.error(e, "Cassandra snapshot failed initialized ...")
        //throw this to let default akka system supervision to restart me
        throw new RuntimeException("Cassandra snapshot failed initialized: " + e.getMessage)
    }

    initialized = true
    log.info("##### Cassandra snapshot successfully lazy initialized ...")
  }

  def loadAsync(processorId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = for {
    mds <- Future(metadata(processorId, criteria).take(3).toVector)
    res <- loadNAsync(mds)
  } yield res

  def loadNAsync(metadata: immutable.Seq[SnapshotMetadata]): Future[Option[SelectedSnapshot]] = metadata match {
    case Seq() => Future.successful(None)
    case md +: mds => load1Async(md) map {
      case Snapshot(s) => Some(SelectedSnapshot(md, s))
    } recoverWith {
      case e => loadNAsync(mds) // try older snapshot
    }
  }

  def load1Async(metadata: SnapshotMetadata): Future[Snapshot] = {
    val stmt = preparedSelectSnapshot.bind(metadata.processorId, metadata.sequenceNr: JLong)
    session.executeAsync(stmt).map(rs => deserialize(rs.one().getBytes("snapshot")))
  }

   def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    val stmt = preparedWriteSnapshot.bind(metadata.processorId, metadata.sequenceNr: JLong, metadata.timestamp: JLong, serialize(Snapshot(snapshot)))
    session.executeAsync(stmt).map(_ => ())
  }

  def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = {
    val stmt = preparedDeleteSnapshot.bind(metadata.processorId, metadata.sequenceNr: JLong)
    session.executeAsync(stmt).map(_ => ())
  }

  def deleteAsync(processorId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = {
    val mds = metadata(processorId, criteria).toVector
    mds.foreach{md => 
      val processorId = md.processorId
      val sequenceNr :JLong = md.sequenceNr
      var psDel = s"DELETE FROM ${tableName} WHERE processor_id = '${processorId}' AND sequence_nr = ${sequenceNr}"
      preparedDeleteSnapshotBatch.append("\n")
      preparedDeleteSnapshotBatch.append(psDel)
    }
    preparedDeleteSnapshotBatch.append("\n")
    preparedDeleteSnapshotBatch.append("APPLY BATCH;")
    executeBatch(preparedDeleteSnapshotBatch.toString)
  }

  def executeBatch(batch:String): Future[Unit] = {
    val stmt = new SimpleStatement(batch).setConsistencyLevel(writeConsistency).asInstanceOf[SimpleStatement]
    session.executeAsync(stmt).map(_ => ())
  }
  
  private def serialize(snapshot: Snapshot): ByteBuffer =
    ByteBuffer.wrap(serialization.findSerializerFor(snapshot).toBinary(snapshot))

  private def deserialize(bytes: ByteBuffer): Snapshot =
    serialization.deserialize(ByteBufferUtil.getArray(bytes), classOf[Snapshot]).get

  private def metadata(processorId: String, criteria: SnapshotSelectionCriteria): Iterator[SnapshotMetadata] =
    new RowIterator(processorId, criteria.maxSequenceNr).map { row =>
      SnapshotMetadata(row.getString("processor_id"), row.getLong("sequence_nr"), row.getLong("timestamp"))
    }.dropWhile(_.timestamp > criteria.maxTimestamp)

  private class RowIterator(processorId: String, maxSequenceNr: Long) extends Iterator[Row] {
    var currentSequenceNr = maxSequenceNr
    var rowCount = 0
    var iter = newIter()

    def newIter() = {
      session.execute(preparedSelectSnapshotMetadataForLoad.bind(processorId, currentSequenceNr: JLong)).iterator
    }

    @annotation.tailrec
    final def hasNext: Boolean =
      if (iter.hasNext)
        true
      else if (rowCount < maxMetadataResultSize)
        false
      else {
        rowCount = 0
        currentSequenceNr -= 1
        iter = newIter()
        hasNext
      }

    def next(): Row = {
      val row = iter.next()
      currentSequenceNr = row.getLong("sequence_nr")
      rowCount += 1
      row
    }
  }

  override def postStop(): Unit = {
    if (null != session) {
      session.shutdown()
    }
    if (null != cluster) {
      cluster.shutdown()
    }
  }
}
