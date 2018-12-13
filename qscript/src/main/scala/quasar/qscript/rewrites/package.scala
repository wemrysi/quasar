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

package quasar.qscript

import slamdata.Predef.{Boolean, None, Option, Some}

import quasar.contrib.iota._
import quasar.fp.ski.κ

import matryoshka._
import matryoshka.data.free._
import matryoshka.implicits._
import matryoshka.patterns._

import scalaz.std.anyVal._
import scalaz.std.option._
import scalaz.syntax.foldable._
import scalaz.syntax.tag._
import scalaz.syntax.std.boolean._

package object rewrites {
  /**
   * Returns whether the given expression has "inner" semantics w.r.t.
   * `Undefined`.
   */
  def isInnerExpr[T[_[_]], A](fm: FreeMapA[T, A]): Boolean = {
    import MapFuncsCore._

    // Boolean = derived from A?
    val alg: AlgebraM[Option, MapFunc[T, ?], Boolean] = {
      case MFC(And(l, r)) => if (l ^ r) None else Some(l)

      case MFC(Or(l, r)) => if (l ^ r) None else Some(l)

      case MFC(ConcatMaps(l, r)) => if (l ^ r) None else Some(l)

      case MFC(ConcatArrays(l, r)) => if (l ^ r) None else Some(l)

      case MFC(IfUndefined(false, r)) => Some(r)
      case MFC(IfUndefined(true, _)) => None

      case other => Some(other.foldMap(_.disjunction).unwrap)
    }

    fm.cataM(interpretM(κ(Some(true)), alg)).isDefined
  }
}
