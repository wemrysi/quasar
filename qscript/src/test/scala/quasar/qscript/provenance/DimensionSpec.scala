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

import slamdata.Predef.{Char, Int}
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

  val P = Prov[Char, I, Fix[ProvF[Char, I, ?]]](stdLeft)
  val D = Dimension(P)

  def i(int: Int): I = Right(int)
  def c(char: Char): I = Left(char)

  def ikey(l: Int, r: Int): JoinKey[I] =
    JoinKey(i(l), i(r))

  import P.implicits._

  "project path on empty dims creates new" >> {
    D.projectPath('q', D.empty) must_= Dimensions.origin(P.prjPath('q'))
  }

  "project static on empty dims is empty" >> {
    D.projectStatic('x', D.empty) must_= D.empty
  }

  "project static key known not to exist is empty" >> {
    val d = D.injectStatic('y', D.lshift(i(1), D.projectStatic('c', D.projectPath('f', D.empty))))
    D.projectStatic('z', d) must_= D.empty
  }

  "project static eliminates inject static" >> {
    val init = Dimensions.origin(
      P.both(P.injValue('x'), P.injValue('y')),
      P.value(i(3)),
      P.prjPath('a'))

    val exp = Dimensions.origin(P.value(i(3)), P.prjPath('a'))

    D.projectStatic('x', init) must_= exp
  }

  "project static eliminates inject static within sequence" >> {
    val init = Dimensions.origin(
      P.thenn(
        P.both(P.injValue('x'), P.injValue('y')),
        P.value(i(3))),
      P.prjPath('a'))

    val exp = Dimensions.origin(P.value(i(3)), P.prjPath('a'))

    D.projectStatic('x', init) must_= exp
  }

  "inject static on empty dims is empty" >> {
    D.injectStatic('x', D.empty) must_= D.empty
  }

  "autojoin keys" >> {
    "builds join keys from zipped dimensions" >> {
      val x = D.lshift(i(1), D.lshift(i(2), D.projectPath('a', D.empty)))
      val y = D.lshift(i(3), D.lshift(i(4), D.projectPath('a', D.empty)))

      val jks = JoinKeys(IList(NonEmptyList(
        NonEmptyList(ikey(1, 3)),
        NonEmptyList(ikey(2, 4)))))

      D.autojoinKeys(x, y) must_= jks
    }

    "flatten joins with shift" >> {
      val x = D.lshift(i(3), D.flatten(i(2), D.lshift(i(1), D.projectPath('a', D.empty))))
      val y = D.lshift(i(9), D.lshift(i(8), D.lshift(i(7), D.projectPath('a', D.empty))))

      val jks = JoinKeys(IList(NonEmptyList(
        NonEmptyList(ikey(1, 7)),
        NonEmptyList(ikey(2, 8)),
        NonEmptyList(ikey(3, 9)))))

      D.autojoinKeys(x, y) must_= jks
    }

    "flatten joins with project" >> {
      val x = D.lshift(i(3), D.projectStatic('k', D.lshift(i(1), D.projectPath('a', D.empty))))
      val y = D.flatten(i(9), D.lshift(i(7), D.projectPath('a', D.empty)))

      val jks = JoinKeys(IList(NonEmptyList(
        NonEmptyList(ikey(1, 7)),
        NonEmptyList(JoinKey(c('k'), i(9))))))

      D.autojoinKeys(x, y) must_= jks
    }

    "shift joins with project" >> {
      val x = D.lshift(i(3), D.projectStatic('k', D.lshift(i(1), D.projectPath('a', D.empty))))
      val y = D.lshift(i(9), D.lshift(i(7), D.projectPath('a', D.empty)))

      val jks = JoinKeys(IList(NonEmptyList(
        NonEmptyList(ikey(1, 7)),
        NonEmptyList(JoinKey(c('k'), i(9))))))

      D.autojoinKeys(x, y) must_= jks
    }

    "halts the join on project mismatch" >> {
      val x = D.lshift(i(2), D.projectStatic('x', D.flatten(i(1), D.projectPath('a', D.empty))))
      val y = D.lshift(i(8), D.projectStatic('y', D.flatten(i(7), D.projectPath('a', D.empty))))

      val jks = JoinKeys(IList(NonEmptyList(NonEmptyList(ikey(1, 7)))))

      D.autojoinKeys(x, y) must_= jks
    }

    "halts the join on path mismatch" >> {
      val x = D.lshift(i(2), D.flatten(i(1), D.projectPath('b', D.projectPath('a', D.empty))))
      val y = D.lshift(i(8), D.flatten(i(7), D.projectPath('c', D.projectPath('a', D.empty))))

      D.autojoinKeys(x, y) must_= JoinKeys.empty
    }

    "join with union disjoins keys for each side" >> {
      val x = D.lshift(i(2), D.projectPath('a', D.empty))
      val y = D.lshift(i(3), D.projectPath('a', D.empty))
      val z = D.lshift(i(4), D.projectPath('a', D.empty))

      val jks = JoinKeys(IList(
        NonEmptyList(NonEmptyList(ikey(2, 4))),
        NonEmptyList(NonEmptyList(ikey(3, 4)))))

      D.autojoinKeys(D.union(x, y), z) must_= jks
    }
  }
}
