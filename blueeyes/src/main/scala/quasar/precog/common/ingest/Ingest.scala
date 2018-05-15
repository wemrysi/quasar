/*
 * Copyright 2014–2018 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.precog.common.ingest

import quasar.blueeyes._, json._, serialization._
import Iso8601Serialization._, Versioned._
import Extractor._
import quasar.precog.common.{JobId, Path}

import scalaz.Validation

import shapeless.HNil

import java.time.Instant

sealed trait Event {
  def fold[A](ingest: Ingest => A): A
}

object Event {
  implicit val decomposer: Decomposer[Event] = new Decomposer[Event] {
    override def decompose(event: Event): JValue = {
      event.fold(_.serialize)
    }
  }
}

case class Ingest(path: Path, data: Seq[JValue], jobId: Option[JobId], timestamp: Instant)
    extends Event {
  def fold[A](ingest: Ingest => A): A = ingest(this)
}

object Ingest {
  val schemaV1 = "path" :: "data" :: "jobId" :: "timestamp" :: HNil
  implicit def seqExtractor[A: Extractor]: Extractor[Seq[A]] = implicitly[Extractor[List[A]]].map(_.toSeq)

  val decomposerV1: Decomposer[Ingest] = decomposerV[Ingest](schemaV1, Some("1.1".v))
  val extractorV1: Extractor[Ingest] = extractorV[Ingest](schemaV1, Some("1.1".v))

  // A transitionary format similar to V1 structure, but lacks a version number and only carries a single data element
  val extractorV1a = new Extractor[Ingest] {
    def validated(obj: JValue): Validation[Error, Ingest] =
      obj.validated[Path]("path").map { path =>
        val jv = (obj \ "data")
        Ingest(
          path,
          if (jv == JUndefined) Vector() else Vector(jv),
          None,
          EventMessage.defaultTimestamp)
      }
  }

  val extractorV0 = new Extractor[Ingest] {
    def validated(obj: JValue): Validation[Error, Ingest] = {
      obj.validated[Path]("path").map { path =>
        val jv = (obj \ "data")
        Ingest(path, if (jv == JUndefined) Vector() else Vector(jv), None, EventMessage.defaultTimestamp)
      }
    }
  }

  implicit val decomposer: Decomposer[Ingest] = decomposerV1
  implicit val extractor: Extractor[Ingest] = extractorV1
}

object EventMessage {
  // an instant that's close enough to the start of timestamping for our purposes
  val defaultTimestamp = instant fromMillis 1362465101979L
}

