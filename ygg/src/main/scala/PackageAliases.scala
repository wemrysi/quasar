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

package ygg.pkg

trait PackageAliases extends quasar.pkg.PackageAliases {
  type CBF[-From, -Elem, +To]         = scala.collection.generic.CanBuildFrom[From, Elem, To]
  type CoGroupResult[K, V, V1, CC[X]] = scala.collection.immutable.Seq[K -> CoGroupValue[V, V1, CC]]
  type CoGroupValue[V, V1, CC[X]]     = scalaz.Either3[V, CC[V] -> CC[V1], V1]
  type LazyPairOf[+A]                 = scalaz.Need[A -> A]
  type NeedStreamT[A]                 = scalaz.StreamT[scalaz.Need, A]

  type jUri         = java.net.URI
  type MaybeSelf[A] = A =?> A
  type ToSelf[A]    = A => A
}
