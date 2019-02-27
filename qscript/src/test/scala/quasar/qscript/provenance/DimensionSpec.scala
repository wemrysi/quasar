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
import quasar.Qspec

import scala.util.{Either, Left, Right}

import matryoshka._
import matryoshka.data.Fix

import monocle.std.either.stdLeft

import scalaz.{IList, NonEmptyList}
import scalaz.std.anyVal._
import scalaz.std.either._

object DimensionSpec extends Qspec {
  type I = Either[Char, Int]

  val P = Prov[Char, I, Boolean, Fix[ProvF[Char, I, Boolean, ?]]](stdLeft)
  val D = Dimension(P)

  def i(int: Int): I = Right(int)
  def c(char: Char): I = Left(char)

  val T: Boolean = true
  val F: Boolean = false

  val B: Fix[ProvF[Char, I, Boolean, ?]] =
    P.fresh()

  val Base: Dimensions[Fix[ProvF[Char, I, Boolean, ?]]] =
    Dimensions.origin(B)

  def ikey(l: Int, r: Int): JoinKey[I] =
    JoinKey(i(l), i(r))

  import P.implicits._

  "project static on empty dims is empty" >> {
    D.projectStatic('x', T, D.empty) must_= D.empty
  }

  "project static key known not to exist is empty" >> {
    val d = D.injectStatic('y', T, D.lshift(i(1), T, D.projectStatic('c', T, Base)))

    D.projectStatic('z', T, d) must_= D.empty
  }

  "project static key of different sort is empty" >> {
    val d = D.injectStatic('y', T, D.lshift(i(1), T, D.projectStatic('c', T, Base)))

    D.projectStatic('y', F, d) must_= D.empty
  }

  "project static eliminates inject static" >> {
    val init = Dimensions.origin(
      P.both(P.inject('x', F), P.inject('y', T)),
      P.inflate(i(3), T),
      B)

    val exp = Dimensions.origin(P.inflate(i(3), T), B)

    D.projectStatic('x', F, init) must_= exp
  }

  "project static eliminates inject static within sequence" >> {
    val init = Dimensions.origin(
      P.thenn(
        P.both(P.inject('x', T), P.inject('y', F)),
        P.inflate(i(3), T)),
      B)

    val exp = Dimensions.origin(P.inflate(i(3), T), B)

    D.projectStatic('x', T, init) must_= exp
  }

  "inject static on empty dims is empty" >> {
    D.injectStatic('x', F, D.empty) must_= D.empty
  }

  "autojoin keys" >> {
    "builds join keys from zipped dimensions" >> {
      val x = D.lshift(i(1), F, D.lshift(i(2), T, Base))
      val y = D.lshift(i(3), F, D.lshift(i(4), T, Base))

      val jks = JoinKeys(IList(NonEmptyList(
        NonEmptyList(ikey(1, 3)),
        NonEmptyList(ikey(2, 4)))))

      D.autojoinKeys(x, y) must_= jks
    }

    "flatten joins with shift of same sort" >> {
      val x = D.lshift(i(3), T, D.flatten(i(2), T, D.lshift(i(1), T, Base)))
      val y = D.lshift(i(9), T, D.lshift(i(8), T, D.lshift(i(7), T, Base)))

      val jks = JoinKeys(IList(NonEmptyList(
        NonEmptyList(ikey(1, 7)),
        NonEmptyList(ikey(2, 8)),
        NonEmptyList(ikey(3, 9)))))

      D.autojoinKeys(x, y) must_= jks
    }

    "flatten crosses with shift of different sort" >> {
      val x = D.lshift(i(3), T, D.flatten(i(2), F, D.lshift(i(1), T, Base)))
      val y = D.lshift(i(9), T, D.lshift(i(8), T, D.lshift(i(7), T, Base)))

      val jks = JoinKeys(IList(NonEmptyList(NonEmptyList(ikey(1, 7)))))

      D.autojoinKeys(x, y) must_= jks
    }

    "flatten joins with project of same sort" >> {
      val x = D.lshift(i(3), T, D.projectStatic('k', T, D.lshift(i(1), T, Base)))
      val y = D.flatten(i(9), T, D.lshift(i(7), T, Base))

      val jks = JoinKeys(IList(NonEmptyList(
        NonEmptyList(ikey(1, 7)),
        NonEmptyList(JoinKey(c('k'), i(9))))))

      D.autojoinKeys(x, y) must_= jks
    }

    "flatten crosses with project of different sort" >> {
      val x = D.lshift(i(3), T, D.projectStatic('k', F, D.lshift(i(1), T, Base)))
      val y = D.flatten(i(9), T, D.lshift(i(7), T, Base))

      val jks = JoinKeys(IList(NonEmptyList(NonEmptyList(ikey(1, 7)))))

      D.autojoinKeys(x, y) must_= jks
    }

    "shift joins with project of same sort" >> {
      val x = D.lshift(i(3), T, D.projectStatic('k', T, D.lshift(i(1), T, Base)))
      val y = D.lshift(i(9), T, D.lshift(i(7), T, Base))

      val jks = JoinKeys(IList(NonEmptyList(
        NonEmptyList(ikey(1, 7)),
        NonEmptyList(JoinKey(c('k'), i(9))))))

      D.autojoinKeys(x, y) must_= jks
    }

    "shift crosses with project of different sort" >> {
      val x = D.lshift(i(3), T, D.projectStatic('k', T, D.lshift(i(1), T, Base)))
      val y = D.lshift(i(9), F, D.lshift(i(7), T, Base))

      val jks = JoinKeys(IList(NonEmptyList(NonEmptyList(ikey(1, 7)))))

      D.autojoinKeys(x, y) must_= jks
    }

    "halts the join on project mismatch" >> {
      val x = D.lshift(i(2), T, D.projectStatic('x', T, D.flatten(i(1), T, Base)))
      val y = D.lshift(i(8), T, D.projectStatic('y', T, D.flatten(i(7), T, Base)))

      val jks = JoinKeys(IList(NonEmptyList(NonEmptyList(ikey(1, 7)))))

      D.autojoinKeys(x, y) must_= jks
    }

    "halts the join on sort mismatch" >> {
      val x = D.lshift(i(2), T, D.projectStatic('y', F, D.flatten(i(1), T, Base)))
      val y = D.lshift(i(8), T, D.projectStatic('y', T, D.flatten(i(7), T, Base)))

      val jks = JoinKeys(IList(NonEmptyList(NonEmptyList(ikey(1, 7)))))

      D.autojoinKeys(x, y) must_= jks
    }

    "join with union disjoins keys for each side" >> {
      val x = D.lshift(i(2), T, Base)
      val y = D.lshift(i(3), T, Base)
      val z = D.lshift(i(4), T, Base)

      val jks = JoinKeys(IList(
        NonEmptyList(NonEmptyList(ikey(2, 4))),
        NonEmptyList(NonEmptyList(ikey(3, 4)))))

      D.autojoinKeys(D.union(x, y), z) must_= jks
    }
  }
}
