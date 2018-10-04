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

package quasar.qscript

import slamdata.Predef.{List, String}
import quasar.RenderTree
import quasar.contrib.pathy.APath

import monocle.macros.Lenses
import pathy.Path.posixCodec
import scalaz.{Equal, Show}
import scalaz.std.list._
import scalaz.std.string._
import scalaz.std.tuple._
import scalaz.syntax.show._
import scalaz.syntax.std.option._

/* LeftShift(
 *   ShiftedRead(path, ExcludeId),
 *   ProjectKey(ProjectKey(Hole, foo), bar),
 *   shiftStatus,
 *   shiftType,
 *   OnUndefined.Omit,
 *   repair) where `repair` does not reference `LeftSide`
 *
 * is equivalent to
 *
 * ExtraShiftedRead(path, List(foo, bar), shiftStatus, shiftType, _)
 */
@Lenses final case class ExtraShiftedRead[A](
  path: A,
  shiftPath: ShiftPath,
  shiftStatus: IdStatus,
  shiftType: ShiftType,
  shiftKey: ShiftKey)

object ExtraShiftedRead {

  implicit def equal[A: Equal]: Equal[ExtraShiftedRead[A]] =
    Equal.equalBy(r =>
      (r.path,
      r.shiftPath,
      r.shiftStatus,
      r.shiftType,
      r.shiftKey))

  implicit def show[A <: APath]: Show[ExtraShiftedRead[A]] =
    RenderTree.toShow

  implicit def renderTree[A <: APath]: RenderTree[ExtraShiftedRead[A]] =
    RenderTree.simple(List("ExtraShiftedRead"), r => {
      (posixCodec.printPath(r.path) + ", " +
        r.shiftPath.shows + ", " +
        r.shiftStatus.shows + ", " +
        r.shiftType.shows + ", " +
        r.shiftKey.shows).some
    })
}

final case class ShiftKey(key: String)

object ShiftKey {
  implicit val equal: Equal[ShiftKey] =
    Equal.equalBy(_.key)

  implicit val show: Show[ShiftKey] =
    Show.showFromToString
}

final case class ShiftPath(path: List[String])

object ShiftPath {
  implicit val equal: Equal[ShiftPath] =
    Equal.equalBy(_.path)

  implicit val show: Show[ShiftPath] =
    Show.showFromToString
}
