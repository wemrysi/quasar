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

package quasar.qscript.provenance

import slamdata.Predef.Boolean
import quasar.Qspec
import DimensionsGenerator._

import scala.Predef.implicitly

import org.scalacheck.{Arbitrary, Properties}
import scalaz.{@@, Equal, SemiLattice, Show}
import scalaz.Tags.{Conjunction, Disjunction}
import scalaz.scalacheck.ScalazProperties.{equal => eql, monoid, traverse}
import scalaz.std.anyVal._
import scalaz.syntax.tag._
import scalaz.syntax.std.boolean._

final class DimensionsSpec extends Qspec {
  import DimensionsSpec._

  "union is idempotent" >> prop { d: Dimensions[Boolean] =>
    (d ∨ d) must_= d
  }

  "union is commutative" >> prop { (l: Dimensions[Boolean], r: Dimensions[Boolean]) =>
    (l ∨ r) must_= (r ∨ l)
  }

  "join is commutative" >> prop { (l: Dimensions[Boolean @@ Conjunction], r: Dimensions[Boolean @@ Conjunction]) =>
    (l ∧ r) must_= (r ∧ l)
  }

  "laws" >> addFragments(properties {
    val p = new Properties("scalaz")

    p.include(eql.laws[Dimensions[Boolean]])

    p.include(
      monoid.laws[Dimensions[Boolean] @@ Disjunction](
        Dimensions.disjMonoid[Boolean], implicitly, implicitly),
      "union.")

    p.include(
      monoid.laws[Dimensions[Boolean @@ Conjunction] @@ Conjunction](
        Dimensions.conjMonoid[Boolean @@ Conjunction], implicitly, implicitly),
      "join.")

    p.include(traverse.laws[Dimensions])

    p
  })
}

object DimensionsSpec {
  implicit val andSemiLattice: SemiLattice[Boolean @@ Conjunction] =
    new SemiLattice[Boolean @@ Conjunction] {
      def append(l: Boolean @@ Conjunction, r: => Boolean @@ Conjunction) =
        (l.unwrap && r.unwrap).conjunction
    }

  implicit val boolConjEqual: Equal[Boolean @@ Conjunction] =
    Conjunction.subst(Equal[Boolean])

  implicit val boolConjShow: Show[Boolean @@ Conjunction] =
    Conjunction.subst(Show[Boolean])

  implicit val boolConjArbitrary: Arbitrary[Boolean @@ Conjunction] =
    Conjunction.subst(implicitly[Arbitrary[Boolean]])

  implicit def dimsConjEqual[A: Equal]: Equal[Dimensions[A] @@ Conjunction] =
    Conjunction.subst(Equal[Dimensions[A]])

  implicit def dimsConjShow[A: Show]: Show[Dimensions[A] @@ Conjunction] =
    Conjunction.subst(Show[Dimensions[A]])

  implicit def dimsConjArbitrary[A: Arbitrary: Equal]: Arbitrary[Dimensions[A] @@ Conjunction] =
    Conjunction.subst(implicitly[Arbitrary[Dimensions[A]]])

  implicit def dimsDisjEqual[A: Equal]: Equal[Dimensions[A] @@ Disjunction] =
    Disjunction.subst(Equal[Dimensions[A]])

  implicit def dimsDisjShow[A: Show]: Show[Dimensions[A] @@ Disjunction] =
    Disjunction.subst(Show[Dimensions[A]])

  implicit def dimsDisjArbitrary[A: Arbitrary: Equal]: Arbitrary[Dimensions[A] @@ Disjunction] =
    Disjunction.subst(implicitly[Arbitrary[Dimensions[A]]])
}
