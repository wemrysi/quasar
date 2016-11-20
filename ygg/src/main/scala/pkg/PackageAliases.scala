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

import scalaz.~>

trait PackageAliases extends quasar.pkg.PackageAliases {
  type ADir        = quasar.contrib.pathy.ADir
  type AFile       = quasar.contrib.pathy.AFile
  type APath       = quasar.contrib.pathy.APath
  type FPath       = quasar.contrib.pathy.FPath
  type PathSegment = quasar.contrib.pathy.PathSegment
  type Sandboxed   = pathy.Path.Sandboxed
  type PathCodec   = pathy.Path.PathCodec

  type AlgebraM[M[_], F[_], A] = matryoshka.AlgebraM[M, F, A]
  type Algebra[F[_], A]        = matryoshka.Algebra[F, A]
  type Coalgebra[F[_], A]      = matryoshka.Coalgebra[F, A]
  type Corecursive[T[_[_]]]    = matryoshka.Corecursive[T]
  type Fix[F[_]]               = matryoshka.Fix[F]
  type Recursive[T[_[_]]]      = matryoshka.Recursive[T]
  type RenderTreeT[T[_[_]]]    = quasar.RenderTreeT[T]
  type ShowT[T[_[_]]]          = quasar.contrib.matryoshka.ShowT[T]
  val Corecursive              = matryoshka.Corecursive
  val Fix                      = matryoshka.Fix
  val Recursive                = matryoshka.Recursive

  type Iso[A, B]     = monocle.Iso[A, B]
  type Lens[A, B]    = monocle.Lens[A, B]
  type OptLens[A, B] = monocle.Optional[A, B]
  type Prism[A, B]   = monocle.Prism[A, B]
  val Iso            = monocle.Iso
  val OptLens        = monocle.Optional
  val Prism          = monocle.Prism

  type Sized[+Repr, L <: Nat] = shapeless.Sized[Repr, L]
  type Nat                    = shapeless.Nat
  val Sized                   = shapeless.Sized

  type EJson[A] = quasar.ejson.EJson[A]
  type LP[A]    = quasar.frontend.logicalplan.LogicalPlan[A]
  type Type     = quasar.Type
  val Type      = quasar.Type

  type AsTask[F[X]]           = Task[F ~> Task]
  type Bytes                  = scala.Array[scala.Byte]
  type CBF[-From, -Elem, +To] = scala.collection.generic.CanBuildFrom[From, Elem, To]
  type LazyPairOf[+A]         = scalaz.Need[PairOf[A]]
  type LocalDateTime          = java.time.LocalDateTime
  type MaybeSelf[A]           = A =?> A
  type Sym                    = scala.Symbol
  type Table                  = ygg.table.TableData
  type Task[A]                = scalaz.concurrent.Task[A]
  type ZonedDateTime          = java.time.ZonedDateTime
  type jUri                   = java.net.URI
  val Task                    = scalaz.concurrent.Task

  implicit class PathyRFPathOps(path: pathy.Path[_, _, Sandboxed]) {
    import pathy.Path.{ rootDir, posixCodec }
    def toAbsolute: APath = quasar.contrib.pathy.mkAbsolute(rootDir, path)
    def toJavaFile: jFile = new jFile(posixCodec unsafePrintPath path)
  }
}
