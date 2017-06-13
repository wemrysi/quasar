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

package quasar.precog

import scala.collection.{ mutable => scm }

trait PackageAliases {
  // scala stdlib
  type ->[+A, +B]           = (A, B)
  type ArrayBuffer[A]       = scm.ArrayBuffer[A]
  type BigDecimal           = scala.math.BigDecimal
  type ListBuffer[A]        = scm.ListBuffer[A]
  type Regex                = scala.util.matching.Regex
  type ScalaMathOrdering[A] = scala.math.Ordering[A] // so many orders
  type scmMap[K, V]         = scm.Map[K, V]
  type scmSet[A]            = scm.Set[A]
  val ArrayBuffer           = scm.ArrayBuffer
  val BigDecimal            = scala.math.BigDecimal
  val ListBuffer            = scm.ListBuffer
  val scmMap                = scm.HashMap
  val scmSet                = scm.HashSet

  // other outside libs: scalaz, spire, shapeless, scodec
  type ByteVector     = scodec.bits.ByteVector
  type IO[A]          = scalaz.effect.IO[A]
  type Iso[T, L]      = shapeless.Generic.Aux[T, L]
  type Logging        = org.slf4s.Logging
  type ScalazOrder[A] = scalaz.Order[A]
  type ScalazOrdering = scalaz.Ordering
  type SpireOrder[A]  = spire.algebra.Order[A]
  type Task[+A]       = scalaz.concurrent.Task[A]

  // It looks like scalaz.Id.Id is (now?) invariant.
  type Id[+X]        = X
  val ScalazOrder    = scalaz.Order
  val ScalazOrdering = scalaz.Ordering
  val IO             = scalaz.effect.IO
}
