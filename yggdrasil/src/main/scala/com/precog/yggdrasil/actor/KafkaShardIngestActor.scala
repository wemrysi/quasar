/*
 *  ____    ____    _____    ____    ___     ____
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either version
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog.yggdrasil
package actor

import metadata.ColumnMetadata
import com.precog.util._
import com.precog.common._
import com.precog.common.accounts._
import com.precog.common.ingest._
import com.precog.common.security._
import ColumnMetadata.monoid

import akka.actor.Actor
import akka.util.duration._

import org.slf4s._

import java.io.{ PrintWriter, BufferedReader, FileWriter, FileReader }
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.atomic.AtomicInteger

import blueeyes._
import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.json.serialization.Extractor._
import blueeyes.json.serialization.DefaultSerialization._

import scalaz.{ NonEmptyList => NEL, _ }
import scalaz.std.list._
import scalaz.syntax.monad._
import scalaz.syntax.monoid._
import scalaz.syntax.traverse._
import Validation._

import scala.collection.immutable.TreeMap

import scalaz._
import scalaz.Validation._
import scalaz.effect._
import scalaz.syntax.apply._
import scalaz.syntax.bifunctor._
import scalaz.syntax.monoid._

//////////////
// MESSAGES //
//////////////

case class GetMessages(sendTo: ActorRef)

sealed trait ShardIngestAction
sealed trait IngestResult extends ShardIngestAction
case class IngestErrors(errors: Seq[String])               extends IngestResult
case class IngestData(messages: Seq[(Long, EventMessage)]) extends IngestResult

case class ProjectionUpdatesExpected(projections: Int)

////////////
// ACTORS //
////////////

trait IngestFailureLog {
  def logFailed(offset: Long, message: EventMessage, lastKnownGood: YggCheckpoint): IngestFailureLog
  def checkFailed(message: EventMessage): Boolean
  def restoreFrom: YggCheckpoint
  def persist: IO[IngestFailureLog]
}

case class FilesystemIngestFailureLog(failureLog: Map[EventMessage, FilesystemIngestFailureLog.LogRecord], restoreFrom: YggCheckpoint, persistDir: File)
    extends IngestFailureLog {
  import scala.math.Ordering.Implicits._
  import FilesystemIngestFailureLog._
  import FilesystemIngestFailureLog.LogRecord._

  def logFailed(offset: Long, message: EventMessage, lastKnownGood: YggCheckpoint): IngestFailureLog = {
    copy(failureLog = failureLog + (message -> LogRecord(offset, message, lastKnownGood)), restoreFrom = lastKnownGood min restoreFrom)
  }

  def checkFailed(message: EventMessage): Boolean = failureLog.contains(message)

  def persist: IO[IngestFailureLog] = IO {
    val logFile = new File(persistDir, FilePrefix + System.currentTimeMillis + ".tmp")
    val out     = new PrintWriter(new FileWriter(logFile))
    try {
      for (rec <- failureLog.values) out.println(rec.serialize.renderCompact)
    } finally {
      out.close()
    }

    this
  }
}

object FilesystemIngestFailureLog {
  val FilePrefix = "ingest_failure_log-"
  def apply(persistDir: File, initialCheckpoint: YggCheckpoint): FilesystemIngestFailureLog = {
    persistDir.mkdirs()
    if (!persistDir.isDirectory) {
      throw new IllegalArgumentException(persistDir + " is not a directory usable for failure logs!")
    }

    def readAll(reader: BufferedReader, into: Map[EventMessage, LogRecord]): Map[EventMessage, LogRecord] = {
      val line = reader.readLine()
      if (line == null) into
      else {
        val logRecord = ((Thrown(_: Throwable)) <-: JParser.parseFromString(line)).flatMap(_.validated[LogRecord]).valueOr(err => sys.error(err.message))

        readAll(reader, into + (logRecord.message -> logRecord))
      }
    }

    val logFiles = persistDir.listFiles.toSeq.filter(_.getName.startsWith(FilePrefix))
    if (logFiles.isEmpty) new FilesystemIngestFailureLog(Map(), initialCheckpoint, persistDir)
    else {
      val reader = new BufferedReader(new FileReader(logFiles.maxBy(_.getName.substring(FilePrefix.length).dropRight(4).toLong)))
      try {
        val failureLog = readAll(reader, Map.empty[EventMessage, LogRecord])
        new FilesystemIngestFailureLog(
          failureLog,
          if (failureLog.isEmpty) initialCheckpoint else failureLog.values.minBy(_.lastKnownGood).lastKnownGood,
          persistDir)
      } finally {
        reader.close()
      }
    }
  }

  case class LogRecord(offset: Long, message: EventMessage, lastKnownGood: YggCheckpoint)
  object LogRecord {
    implicit val decomposer: Decomposer[LogRecord] = new Decomposer[LogRecord] {
      def decompose(rec: LogRecord) = JObject(
        "offset"        -> rec.offset.serialize,
        "messageType"   -> rec.message.fold(_ => "ingest", _ => "archive", _ => "storeFile").serialize,
        "message"       -> rec.message.serialize,
        "lastKnownGood" -> rec.lastKnownGood.serialize
      )
    }

    implicit val extractor: Extractor[LogRecord] = new Extractor[LogRecord] {
      def validated(jv: JValue) = {
        for {
          offset <- jv.validated[Long]("offset")
          msgType <- jv.validated[String]("messageType")
          message <- msgType match {
                      case "ingest" =>
                        jv.validated[EventMessage.EventMessageExtraction]("message")(IngestMessage.Extractor).flatMap {
                          _.map(Success(_)).getOrElse(Failure(Invalid("Incomplete ingest message")))
                        }

                      case "archive" => jv.validated[ArchiveMessage]("message")
                    }
          checkpoint <- jv.validated[YggCheckpoint]("lastKnownGood")
        } yield LogRecord(offset, message, checkpoint)
      }
    }
  }
}
