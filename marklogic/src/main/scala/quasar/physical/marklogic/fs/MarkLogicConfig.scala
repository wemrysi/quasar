/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.physical.marklogic.fs

import quasar.Predef._
import quasar.physical.marklogic.MonadErrMsgs

import java.net.URI

import monocle.macros.Lenses
import scalaz._, Scalaz._

@Lenses
final case class MarkLogicConfig(xccUri: URI, docType: MarkLogicConfig.DocType)

object MarkLogicConfig {
  sealed abstract class DocType {
    def fold[A](json: => A, xml: => A): A =
      this match {
        case DocType.Json => json
        case DocType.Xml  => xml
      }
  }

  object DocType {
    case object Json extends DocType
    case object Xml  extends DocType

    val json: DocType = Json
    val xml:  DocType = Xml

    implicit val equal: Equal[DocType] =
      Equal.equalA

    implicit val show: Show[DocType] =
      Show.showFromToString
  }

  def fromUriString[F[_]: MonadErrMsgs](str: String): F[MarkLogicConfig] = {
    def ensureScheme(u: URI): ValidationNel[String, Unit] =
      Option(u.getScheme).exists(_ === "xcc").unlessM("Unrecognized URI scheme, expected 'xcc'.".failureNel)

    def extractDocType(u: URI): ValidationNel[String, DocType] =
      Option(u.getQuery).toList
        .flatMap(_ split '&')
        .flatMap(_ split '=' match {
          case Array(n, v) => List((n, v))
          case other       => List()
        })
        .find(_._1 === "format")
        .fold(DocType.xml.successNel[String]) {
          case (_, "json") => DocType.json.successNel[String]
          case (_, "xml")  => DocType.xml.successNel[String]
          case (_, other)  => s"Unsupported document format: $other".failureNel[DocType]
        }

    \/.fromTryCatchNonFatal(new URI(str))
      .leftMap(_.getMessage.wrapNel)
      .flatMap(u => (ensureScheme(u) *> extractDocType(u) map (MarkLogicConfig(u, _))).disjunction)
      .fold(_.raiseError[F, MarkLogicConfig], _.point[F])
  }
}
