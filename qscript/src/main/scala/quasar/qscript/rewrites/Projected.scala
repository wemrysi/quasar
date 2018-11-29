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

package quasar.qscript.rewrites

import slamdata.Predef.{Option, Some}

import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns.interpret

import quasar.common.CPath
import quasar.contrib.iota._
import quasar.contrib.scalaz.free._
import quasar.qscript._
import quasar.qscript.RecFreeS._

import scalaz.Foldable
import scalaz.std.anyVal._
import scalaz.std.tuple._
import scalaz.syntax.tag._
import scalaz.syntax.std.boolean._

/** Matches on a `FreeMap` where all direct access of `Hole` is via static projection. */
object Projected {
  import MapFuncsCore.{IntLit, ProjectIndex, ProjectKey, StrLit}

  def unapply[T[_[_]]: BirecursiveT](fm: FreeMap[T]): Option[FreeMapA[T, CPath]] =
    projected(fm)

  def projected[T[_[_]]: BirecursiveT](fm: FreeMap[T]): Option[FreeMapA[T, CPath]] = {
    val xformHole: Hole => FreeMapA[T, CPath] =
      _ => FreeA(CPath.Identity)

    val xformFunc: Algebra[MapFunc[T, ?], FreeMapA[T, CPath]] = {
      case MFC(ProjectKey(FreeA(p), StrLit(key))) =>
        FreeA(p \ key)

      case MFC(ProjectIndex(FreeA(p), IntLit(i))) if i.isValidInt =>
        FreeA(p \ i.toInt)

      case other =>
        FreeF(other)
    }

    Some(fm.cata(interpret(xformHole, xformFunc))) filter { r =>
      val (cnt, allPrj) = Foldable[FreeMapA[T, ?]].foldMap(r) { p =>
        (1, (p != CPath.Identity).conjunction)
      }
      cnt > 0 && allPrj.unwrap
    }
  }
}

object ProjectedRec {
  def unapply[T[_[_]]: BirecursiveT](fm: RecFreeMap[T]): Option[FreeMapA[T, CPath]] =
    Projected.projected(fm.linearize)
}
