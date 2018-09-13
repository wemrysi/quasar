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

package quasar.impl.parse

import slamdata.Predef._

import quasar.common.data.PreciseKeys
import quasar.time.DateTimeUtil._

import jawn.{Facade, FContext}
import qdata.QDataEncode
import scalaz.{\/, -\/, \/-}
import scalaz.Scalaz._

import java.lang.CharSequence

object QDataPreciseFacade {

  @SuppressWarnings(Array(
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Equals",
    "org.wartremover.warts.Null",
    "org.wartremover.warts.ToString",
    "org.wartremover.warts.Var"))
  implicit def qdataPrecise[J](implicit qd: QDataEncode[J]): Facade[J] = {
    import QDataFacade.{qdata => facade}

    new Facade[J] {
      def jnull(): J = facade.jnull()

      def jfalse(): J = facade.jfalse()
      def jtrue(): J = facade.jtrue()

      def jnum(s: CharSequence, decIndex: Int, expIndex: Int): J =
        facade.jnum(s, decIndex, expIndex)

      def jstring(s: CharSequence): J = facade.jstring(s)

      def singleContext(): FContext[J] = facade.singleContext()
      def arrayContext(): FContext[J] = facade.arrayContext()

      def objectContext(): FContext[J] =
        new FContext[J] {
          import PreciseKeys._

          var result: qd.NascentObject \/ J = qd.prepObject.left[J]
          var key: String = null

          def setPrecise(str: String): Unit = {
            key = str
            result = null.asInstanceOf[qd.NascentObject \/ J]
          }

          def add(s: CharSequence): Unit = {
            if (key == null) { // we are parsing a key
              val str = s.toString

              str match {
                case `OffsetDateTimeKey` |
                  `OffsetDateKey` |
                  `OffsetTimeKey` |
                  `LocalDateTimeKey` |
                  `LocalDateKey` |
                  `LocalTimeKey` |
                  `IntervalKey` => setPrecise(str)
                case str => key = str
              }
            } else if (result == null) { // we are parsing a precise value
              key match {
                case `OffsetDateTimeKey` =>
                  result = qd.makeOffsetDateTime(parseOffsetDateTime(s.toString)).right[qd.NascentObject]
                case `OffsetDateKey` =>
                  result = qd.makeOffsetDate(parseOffsetDate(s.toString)).right[qd.NascentObject]
                case `OffsetTimeKey` =>
                  result = qd.makeOffsetTime(parseOffsetTime(s.toString)).right[qd.NascentObject]
                case `LocalDateTimeKey` =>
                  result = qd.makeLocalDateTime(parseLocalDateTime(s.toString)).right[qd.NascentObject]
                case `LocalDateKey` =>
                  result = qd.makeLocalDate(parseLocalDate(s.toString)).right[qd.NascentObject]
                case `LocalTimeKey` =>
                  result = qd.makeLocalTime(parseLocalTime(s.toString)).right[qd.NascentObject]
                case `IntervalKey` =>
                  result = qd.makeInterval(parseInterval(s.toString)).right[qd.NascentObject]
              }
              key = null
            } else { // we are parsing a non-precise value
              val newResult: qd.NascentObject = result match {
                case -\/(obj) => obj
                case \/-(_) => scala.sys.error("nope")
              }
              result = qd.pushObject(key, jstring(s), newResult).left[J]
              key = null
            }
          }

          def add(v: J): Unit = {
            val newResult: qd.NascentObject = result match {
              case -\/(obj) => obj
              case \/-(_) => scala.sys.error("nope")
            }
            result = qd.pushObject(key, v, newResult).left[J]
            key = null
          }

          def finish: J = result match {
            case -\/(r) => qd.makeObject(r)
            case \/-(j) => j
          }

          def isObj: Boolean = true
        }
    }
  }
}
