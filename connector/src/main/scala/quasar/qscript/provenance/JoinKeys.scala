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

import monocle.macros.Lenses
import scalaz.{IList, Monoid, PlusEmpty}

final case class JoinKeys[I](keys: IList[JoinKeys.JoinKey[I]])

object JoinKeys extends JoinKeysInstances {
  @Lenses
  final case class JoinKey[I](left: I, right: I)

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
}
