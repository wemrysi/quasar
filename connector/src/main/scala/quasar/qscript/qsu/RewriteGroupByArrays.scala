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

package quasar.qscript.qsu

import quasar.NameGenerator
import quasar.qscript.{SrcHole, LeftSide, MapFuncsCore, MFC, RightSide}
import quasar.qscript.qsu.{QScriptUniform => QSU}
import slamdata.Predef._

import matryoshka.BirecursiveT
import scalaz.{Monad, StateT}
import scalaz.std.list._
import scalaz.syntax.traverse._

final class RewriteGroupByArrays[T[_[_]]: BirecursiveT] private () extends QSUTTypes[T] {
  import QSUGraph.withName
  import QSUGraph.Extractors._

  // recognize the pattern generated in LP of squishing groupbys together
  def apply[F[_]: Monad: NameGenerator](qgraph: QSUGraph): F[QSUGraph] = {
    type G[A] = StateT[F, QSUGraph.RevIdx[T], A]

    val back = qgraph rewriteM {
      case qgraph @ GroupBy(target, NAryArray(keys @ _*)) =>
        val nestedM = keys.toList.foldLeftM(target) { (inner, key) =>
          withName[T, G](QSU.GroupBy[T, Symbol](inner.root, key.root)).map(_ :++ inner)
        }

        nestedM.map(nested => qgraph.overwriteAtRoot(nested.unfold.map(_.root)) :++ nested)
    }

    back.eval(qgraph.generateRevIndex)
  }

  object NAryArray {

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def unapplySeq(qgraph: QSUGraph): Option[Vector[QSUGraph]] = qgraph match {
      case AutoJoin2C(
        NAryArray(elems @ _*),
        Unary(last, MFC(MapFuncsCore.MakeArray(SrcHole))),
        MapFuncsCore.ConcatArrays(LeftSide, RightSide)) => Some(elems.toVector :+ last)

      case Unary(last, MFC(MapFuncsCore.MakeArray(SrcHole))) => Some(Vector(last))

      case _ => None
    }
  }
}

object RewriteGroupByArrays {
  def apply[T[_[_]]: BirecursiveT]: RewriteGroupByArrays[T] = new RewriteGroupByArrays[T]
}
