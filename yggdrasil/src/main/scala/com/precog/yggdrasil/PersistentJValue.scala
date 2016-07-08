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
package com.precog
package yggdrasil

import com.precog.common._
import blueeyes.json._
import org.objectweb.howl.log._

import java.io.FileOutputStream
import java.util.Arrays

object PersistentJValue {
  sealed abstract class Message(val Flag: Byte) {
    def apply(bytes0: Array[Byte]): Array[Byte] = {
      val bytes = new Array[Byte](bytes0.length + 1)
      ByteBufferWrap(bytes).put(bytes0).put(Flag)
      bytes
    }

    def unapply(bytes: Array[Byte]): Option[Array[Byte]] = {
      if (bytes.length > 0 && bytes(bytes.length - 1) == Flag) {
        Some(Arrays.copyOf(bytes, bytes.length - 1))
      } else {
        None
      }
    }
  }

  object Update  extends Message(1: Byte)
  object Written extends Message(2: Byte)

  private def open(baseDir: File, fileName: String): Logger = {
    val config = new Configuration()
    config.setLogFileDir(baseDir.getCanonicalPath)
    config.setLogFileName(fileName)
    config.setLogFileExt("log")
    config.setLogFileMode("rwd")
    config.setChecksumEnabled(true)
    val log = new Logger(config)
    log.open()
    log
  }
}

final case class PersistentJValue(baseDir: File, fileName: String) extends org.slf4s.Logging {
  import PersistentJValue._

  private val hlog       = open(baseDir, fileName)
  private val file: File = new File(baseDir, fileName)
  private var jv: JValue = JUndefined

  replay()

  /** Returns the persisted `JValue`. */
  def json: JValue = jv

  /** Updates and persists (blocking) the `JValue`. */
  def json_=(value: JValue) { jv = value; flush() }

  def close() = hlog.close()

  private def flush() {
    val rawJson = jv.renderCompact.getBytes("UTF-8")
    val mark    = hlog.put(Update(rawJson), true)

    val out = new FileOutputStream(file)
    out.write(rawJson)
    out.close()
    hlog.put(Written(fileName.getBytes("UTF-8")), true)

    hlog.mark(mark, true)
  }

  private def replay() {
    var pending: Option[Array[Byte]]    = None
    var lastUpdate: Option[Array[Byte]] = None

    hlog.replay(new ReplayListener {
      def getLogRecord: LogRecord         = new LogRecord(1024 * 64)
      def onError(ex: LogException): Unit = throw ex
      def onRecord(rec: LogRecord): Unit = rec.`type` match {
        case LogRecordType.END_OF_LOG =>
          log.debug("Versions TX log replay complete in " + baseDir.getCanonicalPath)

        case LogRecordType.USER =>
          val bytes = rec.getFields()(0)
          bytes match {
            case Update(rawJson) =>
              pending = Some(rawJson)
            case Written(rawPath) =>
              lastUpdate = Some(rawPath)
              pending = None
            case _ =>
              sys.error("Found unknown user record!")
          }

        case other =>
          log.warn("Unknown LogRecord type: " + other)
      }
    })

    (pending, lastUpdate) match {
      case (None, Some(_)) =>
        jv = JParser.parseFromFile(file).valueOr(throw _)

      case (Some(rawJson), _) =>
        jv = JParser.parseFromString(new String(rawJson, "UTF-8")).valueOr(throw _)
        flush()

      case (None, None) =>
        flush()
    }
  }
}
