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

package quasar.qscript

import slamdata.Predef.{Map => SMap, _}
import quasar.fp._
import quasar.qscript.provenance.Dimensions

import matryoshka._
import matryoshka.data.free._
import scalaz.{Free, Show}
import scalaz.std.list._
import scalaz.std.string._
import scalaz.syntax.foldable._
import scalaz.syntax.show._

package object qsu {
  type FreeAccess[T[_[_]], A] = FreeMapA[T, Access[A]]
  type QSUDims[T[_[_]]] = SMap[Symbol, Dimensions[QProv.P[T]]]
  type QSUVerts[T[_[_]]] = SMap[Symbol, QScriptUniform[T, Symbol]]

  def AccessValueF[T[_[_]], A](a: A): FreeAccess[T, A] =
    Free.pure[MapFunc[T, ?], Access[A]](Access.Value(a))

  def AccessValueHoleF[T[_[_]]]: FreeAccess[T, Hole] =
    AccessValueF[T, Hole](SrcHole)

  object QSUDims {
    def show[T[_[_]]: ShowT]: Show[QSUDims[T]] =
      Show.shows { dims =>
        "QSUDims[\n" +
        dims.toList.map({ case (k, v) => s"  ${k.shows} -> ${v.shows}"}).intercalate("\n") +
        "\n]"
      }
  }
}
