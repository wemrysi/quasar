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

package quasar.api

import slamdata.Predef.Option
import quasar.CIString

import monocle.macros.Lenses
import monocle.function.At
import scalaz.{Cord, IMap, Order, Show}
import scalaz.syntax.show._
import scalaz.std.option._
import scalaz.std.tuple._

/** Media Types as defined in
  * https://tools.ietf.org/html/rfc2045
  * https://tools.ietf.org/html/rfc6838
  */
@Lenses
final case class MediaType(
    topType: CIString,
    subType: CIString,
    suffix: Option[CIString],
    parameters: MediaType.Parameters)

object MediaType extends MediaTypeInstances {
  type Parameters = IMap[CIString, CIString]
}

sealed abstract class MediaTypeInstances {
  import MediaType.{parameters, Parameters}

  implicit val parametersAt: At[MediaType, CIString, Option[CIString]] =
    new At[MediaType, CIString, Option[CIString]] {
      def at(k: CIString) =
        parameters.composeLens(At.at[Parameters, CIString, Option[CIString]](k))
    }

  implicit val order: Order[MediaType] =
    Order.orderBy(mt => (mt.topType, mt.subType, mt.suffix, mt.parameters))

  implicit val show: Show[MediaType] =
    Show.show {
      case MediaType(tt, st, suf, ps) =>
        val params = ps.foldrWithKey(Cord.empty) { (k, v, c) =>
          Cord(";") ++ k.show ++ Cord("=") ++ v.show ++ c
        }
        tt.show ++ Cord("/") ++ st.show ++ suf.fold(Cord.empty)(s => Cord(s.value)) ++ params
    }
}
