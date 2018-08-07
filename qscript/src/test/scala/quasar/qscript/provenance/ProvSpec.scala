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

import slamdata.Predef.{Boolean, Char, Int}
import quasar.contrib.matryoshka.arbitrary._
import quasar.Qspec

import matryoshka.data.Fix
import org.specs2.scalacheck._
import scalaz.{IList, NonEmptyList, Writer}
import scalaz.std.anyVal._
import scalaz.std.tuple._
import scalaz.syntax.equal._

object ProvSpec extends Qspec with ProvFGenerator {

  implicit val params = Parameters(maxSize = 10)

  val P = Prov[Char, Int, Fix[ProvF[Char, Int, ?]]]

  import P.provenanceEqual

  val p1 = P.prjPath('x')
  val p2 = P.prjPath('y')

  val pk1 = P.prjValue('a')
  val pk2 = P.prjValue('b')

  val ik1 = P.injValue('a')
  val ik2 = P.injValue('b')

  val v1i = 1
  val v1 = P.value(v1i)
  val v2i = 2
  val v2 = P.value(v2i)
  val v3i = 3
  val v3 = P.value(v3i)
  val v4i = 4
  val v4 = P.value(v4i)

  def autojoined(a: Fix[P.PF], b: Fix[P.PF]): (JoinKeys[Int], Boolean) =
    P.autojoined[Writer[JoinKeys[Int], ?]](a, b).run

  "normalization" >> {
    "right assoc boths" >> {
      val tree = P.both(P.both(p1, pk1), P.both(p2, pk2))
      val exp = P.both(p1, P.both(pk1, P.both(p2, pk2)))

      P.normalize(tree) must_= exp
    }

    "distinct both" >> {
      val dupes = P.both(P.both(p1, pk2), P.both(P.both(pk2, p2), p1))
      val exp = P.both(p1, P.both(pk2, p2))

      P.normalize(dupes) must_= exp
    }

    "right assoc oneofs" >> {
      val tree = P.oneOf(P.oneOf(p1, pk1), P.oneOf(p2, pk2))
      val exp = P.oneOf(p1, P.oneOf(pk1, P.oneOf(p2, pk2)))

      P.normalize(tree) must_= exp
    }

    "distinct oneOf" >> {
      val dupes = P.oneOf(P.oneOf(p1, pk2), P.oneOf(P.oneOf(pk2, p2), p1))
      val exp = P.oneOf(p1, P.oneOf(pk2, p2))

      P.normalize(dupes) must_= exp
    }

    "prj(k) << inj(k) << p == p" >> {
      P.normalize(P.thenn(pk1, P.thenn(ik1, p1))) must_= p1
    }

    "prj(k) << inj(k) /\\\\ p << q == (q /\\\\ (prj(k) << p << q))" >> {
      val tree = P.thenn(pk1, P.thenn(P.both(ik1, p2), p1))
      val exp = P.both(p1, P.thenn(pk1, P.thenn(p2, p1)))

      P.normalize(tree) must_= exp
    }

    "prj(k) << inj(k) \\// p << q == (q \\// (prj(k) << p << q))" >> {
      val tree = P.thenn(pk1, P.thenn(P.oneOf(ik1, p2), p1))
      val exp = P.oneOf(p1, P.thenn(pk1, P.thenn(p2, p1)))

      P.normalize(tree) must_= exp
    }

    "prj(k) << inj(j) << q == ∅" >> prop { (k: Char, j: Char, p: Fix[P.PF]) => (k =/= j) ==> {
      val tree = P.thenn(P.prjValue(k), P.thenn(P.injValue(j), p))
      P.normalize(tree) must_= P.nada()
    }}
  }

  "autojoined" >> {
    "p1 θ p1" >> {
      autojoined(p1, p1) must_= ((JoinKeys.empty, true))
    }

    "p1 θ p2" >> {
      autojoined(p1, p2) must_= ((JoinKeys.empty, false))
    }

    "pk1 θ pk1" >> {
      autojoined(pk1, pk1) must_= ((JoinKeys.empty, true))
    }

    "pk1 θ pk2" >> {
      autojoined(pk1, pk2) must_= ((JoinKeys.empty, false))
    }

    "prjPath θ prjValue" >> {
      autojoined(p1, pk1) must_= ((JoinKeys.empty, false))
    }

    "value θ prjPath" >> {
      autojoined(v1, p1) must_= ((JoinKeys.empty, false))
    }

    "value θ prjValue" >> {
      autojoined(v1, pk1) must_= ((JoinKeys.empty, false))
    }

    "value θ value" >> {
      autojoined(v1, v2) must_= ((JoinKeys.singleton(v1i, v2i), true))
    }

    "(v1 /\\\\ v2) θ v3" >> {
      val keys =
        JoinKeys(IList(NonEmptyList(
          JoinKey(v1i, v3i),
          JoinKey(v2i, v3i))))

      autojoined(P.both(v1, v2), v3) must_= ((keys, true))
    }

    "(v1 /\\\\ v2) θ (v3 /\\\\ v4)" >> {
      val keys =
        JoinKeys(IList(NonEmptyList(
          JoinKey(v1i, v3i),
          JoinKey(v1i, v4i),
          JoinKey(v2i, v3i),
          JoinKey(v2i, v4i))))

      autojoined(P.both(v1, v2), P.both(v3, v4)) must_= ((keys, true))
    }

    "(v1 /\\\\ v2) θ (v3 \\// v4)" >> {
      val keys =
        JoinKeys(IList(NonEmptyList(
          JoinKey(v1i, v3i),
          JoinKey(v1i, v4i),
          JoinKey(v2i, v3i),
          JoinKey(v2i, v4i))))

      autojoined(P.both(v1, v2), P.oneOf(v3, v4)) must_= ((keys, true))
    }

    "(v1 /\\\\ v2) θ (v3 << v4)" >> {
      val keys =
        JoinKeys(IList(NonEmptyList(
          JoinKey(v1i, v4i),
          JoinKey(v2i, v4i))))

      autojoined(P.both(v1, v2), P.thenn(v3, v4)) must_= ((keys, false))
    }

    "(v2 << v1) θ v3" >> {
      autojoined(P.thenn(v2, v1), v3) must_= ((JoinKeys.singleton(v1i, v3i), false))
    }

    "(v2 << v1) θ (v4 << v3)" >> {
      val keys =
        JoinKeys(IList(
          NonEmptyList(JoinKey(v1i, v3i)),
          NonEmptyList(JoinKey(v2i, v4i))))

      autojoined(P.thenn(v2, v1), P.thenn(v4, v3)) must_= ((keys, true))
    }

    "(pk1 << v1) θ (pk1 << v3)" >> {
      autojoined(P.thenn(pk1, v1), P.thenn(pk1, v3)) must_= ((JoinKeys.singleton(v1i, v3i), true))
    }

    "(pk1 << v1) θ (pk2 << v3)" >> {
      autojoined(P.thenn(pk1, v1), P.thenn(pk2, v3)) must_= ((JoinKeys.singleton(v1i, v3i), false))
    }

    "(∅ \\// v2) θ v3" >> {
      autojoined(P.oneOf(P.nada(), v2), v3) must_= ((JoinKeys.singleton(v2i, v3i), true))
    }

    "(∅ \\// v2) θ (v3 \\// ∅)" >> {
      autojoined(P.oneOf(P.nada(), v2), P.oneOf(v3, P.nada())) must_= ((JoinKeys.singleton(v2i, v3i), true))
    }

    "(v1 \\// v2) θ (v3 \\// v4)" >> {
      val keys =
        JoinKeys(IList(NonEmptyList(
          JoinKey(v1i, v3i),
          JoinKey(v1i, v4i),
          JoinKey(v2i, v3i),
          JoinKey(v2i, v4i))))

      autojoined(P.oneOf(v1, v2), P.oneOf(v3, v4)) must_= ((keys, true))
    }

    "(v1 \\// ∅) θ (v4 << v3)" >> {
      autojoined(P.oneOf(v1, P.nada()), P.thenn(v4, v3)) must_= ((JoinKeys.singleton(v1i, v3i), false))
    }

    "((v1 /\\\\ v2) \\// v3) θ v4" >> {
      val keys =
        JoinKeys(IList(NonEmptyList(
          JoinKey(v1i, v4i),
          JoinKey(v2i, v4i),
          JoinKey(v3i, v4i))))

      autojoined(P.oneOf(P.both(v1, v2), v3), v4) must_= ((keys, true))
    }

    "((∅ \\// v2) /\\\\ v3) θ v4" >> {
      val keys =
        JoinKeys(IList(NonEmptyList(
          JoinKey(v2i, v4i),
          JoinKey(v3i, v4i))))

      autojoined(P.both(P.oneOf(P.nada(), v2), v3), v4) must_= ((keys, true))
    }
  }
}
