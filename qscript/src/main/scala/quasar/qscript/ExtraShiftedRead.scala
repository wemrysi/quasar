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
import scalaz.std.string._
import scalaz.std.tuple._
import scalaz.syntax.show._
import scalaz.syntax.std.option._

/* LeftShift(
 *   ShitedRead(path, ExcludeId),
 *   Hole,
 *   shiftStatus,
 *   ShiftType.Map,
 *   OnUndefined.Omit,
 *   repair) where `repair` does not reference `LeftSide`
 *
 * is equivalent to
 *
 * ExtraShiftedRead(path, shiftStatus, _)
 */
@Lenses final case class ExtraShiftedRead[A](
  path: A,
  shiftStatus: IdStatus,
  shiftKey: ShiftKey)

object ExtraShiftedRead {

  implicit def equal[A: Equal]: Equal[ExtraShiftedRead[A]] =
    Equal.equalBy(r => (r.path, r.shiftStatus, r.shiftKey.key))

  implicit def show[A <: APath]: Show[ExtraShiftedRead[A]] =
    RenderTree.toShow

  implicit def renderTree[A <: APath]: RenderTree[ExtraShiftedRead[A]] =
    RenderTree.simple(List("ExtraShiftedRead"), r => {
      (posixCodec.printPath(r.path) + ", " +
        r.shiftStatus.shows + ", " +
        r.shiftKey.key.shows).some
    })
}

final case class ShiftKey(key: String)
