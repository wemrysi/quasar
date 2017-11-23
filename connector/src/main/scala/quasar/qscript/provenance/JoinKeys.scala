/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.qscript.provenance

import slamdata.Predef.StringContext

import monocle.macros.Lenses
import scalaz.{Equal, IList, Monoid, Order, PlusEmpty, Show}
import scalaz.std.tuple._
import scalaz.syntax.show._

final case class JoinKeys[I](keys: IList[JoinKeys.JoinKey[I]])

object JoinKeys extends JoinKeysInstances {
  @Lenses
  final case class JoinKey[I](left: I, right: I)

  object JoinKey extends JoinKeyInstances {
    implicit def order[I: Order]: Order[JoinKey[I]] =
      Order.orderBy(k => (k.left, k.right))

    implicit def show[I: Show]: Show[JoinKey[I]] =
      Show.shows(k => s"JoinKey(${k.left.shows}, ${k.right.shows})")
  }

  sealed abstract class JoinKeyInstances {
    implicit def equal[I: Equal]: Equal[JoinKey[I]] =
      Equal.equalBy(k => (k.left, k.right))
  }

  def singleton[I](l: I, r: I): JoinKeys[I] =
    JoinKeys(IList(JoinKey(l, r)))
}

sealed abstract class JoinKeysInstances {
  implicit def plusEmpty: PlusEmpty[JoinKeys] =
    new PlusEmpty[JoinKeys] {
      def empty[A]: JoinKeys[A] =
        JoinKeys(IList())

      def plus[A](x: JoinKeys[A], y: => JoinKeys[A]): JoinKeys[A] =
        JoinKeys(x.keys ++ y.keys)
    }

  implicit def monoid[I]: Monoid[JoinKeys[I]] =
    plusEmpty.monoid[I]


  implicit def order[I: Order]: Order[JoinKeys[I]] =
    Order.orderBy(_.keys)

  implicit def show[I: Show]: Show[JoinKeys[I]] =
    Show.shows(jks => "JoinKeys" + jks.keys.shows)
}

sealed abstract class JoinKeysInstances0 {
  implicit def equal[I: Equal]: Equal[JoinKeys[I]] =
    Equal.equalBy(_.keys)
}
