/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.qsu
package minimizers

import slamdata.Predef._

import quasar.RenderTree
import quasar.fp.symbolOrder

import scalaz.{Equal, Show}
import scalaz.std.anyVal._
import scalaz.std.option._
import scalaz.std.tuple._

private[minimizers] sealed trait CartoucheRef extends Product with Serializable {
  val ref: Symbol

  def withRef(f: Symbol => Symbol): CartoucheRef =
    this match {
      case CartoucheRef.Final(r) => CartoucheRef.Final(f(r))
      case CartoucheRef.Offset(r, o) => CartoucheRef.Offset(f(r), o)
    }

  def setRef(to: Symbol): CartoucheRef =
    withRef(_ => to)
}

private[minimizers] object CartoucheRef {

  final case class Final(ref: Symbol) extends CartoucheRef

  // offset from top of cartouche (head of list) which contains an IncludeId
  final case class Offset(ref: Symbol, offset: Int) extends CartoucheRef

  implicit def renderTree: RenderTree[CartoucheRef] =
    RenderTree.fromShow("CartoucheRef")

  implicit def show: Show[CartoucheRef] =
    Show.showFromToString

  implicit def equal: Equal[CartoucheRef] =
    Equal.equalBy[CartoucheRef, (Symbol, Option[Int])] {
      case Final(r) => (r, None)
      case Offset(r, o) => (r, Some(o))
    }
}
