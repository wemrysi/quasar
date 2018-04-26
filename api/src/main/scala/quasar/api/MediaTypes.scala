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

import scalaz.{ISet, Order, Semigroup, Show}
import scalaz.syntax.show._

final class MediaTypes private (val element: MediaType, rest: ISet[MediaType]) {
  def ++ (other: MediaTypes): MediaTypes =
    new MediaTypes(element, rest union other.toISet)

  def toISet: ISet[MediaType] =
    rest.insert(element)
}

object MediaTypes extends MediaTypesInstances {
  def apply(elt: MediaType, rest: ISet[MediaType]): MediaTypes =
    new MediaTypes(elt, rest)
}

sealed abstract class MediaTypesInstances {
  implicit val order: Order[MediaTypes] =
    Order.orderBy(_.toISet)

  implicit val semigroup: Semigroup[MediaTypes] =
    Semigroup.instance((a, b) => a ++ b)

  implicit val show: Show[MediaTypes] =
    Show.show(_.toISet.show)
}
