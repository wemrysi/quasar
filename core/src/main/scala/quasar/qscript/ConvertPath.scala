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

package quasar.qscript

import quasar.Predef._
import quasar.qscript.MapFuncs._
import quasar.fp._
import quasar.fs._

import matryoshka._
import matryoshka.patterns._
import pathy.Path, Path.{AbsDir, AbsFile, dir1, file1, rootDir, Sandboxed}
import scalaz._, Scalaz._

// NB: Should be a type class, except for it being multi-parameter
trait ConvertPath[T[_[_]], F[_]] {
  import ConvertPath._

  type H[A]

  def convertPath[M[_]: Monad](f: ListContents[M]): StaticPathTransformation[T, M, F, H]

  def fileType[M[_]: Monad](listContents: ListContents[M]):
      (AbsDir[Sandboxed], String) => OptionT[M, AbsDir[Sandboxed] \/ AbsFile[Sandboxed]] =
    (dir, name) => listContents(dir).toOption >>=
      (cont => OptionT((cont.find(_.fold(_.value ≟ name, _.value ≟ name)) ∘
        (_.bimap(dir </> dir1(_), dir </> file1(_)))).point[M]))
}

object ConvertPath {
  type Aux[T[_[_]], F[_], G[_]] = ConvertPath[T, F] { type H[A] = G[A] }

  type ListContents[M[_]] = AbsDir[Sandboxed] => EitherT[M, FileSystemError, Set[PathSegment]]

  def apply[T[_[_]], F[_]](implicit ev: ConvertPath[T, F]): ConvertPath[T, F] = ev

  implicit def deadEnd[T[_[_]], G[_]]: ConvertPath.Aux[T, Const[DeadEnd, ?], G] =
    new ConvertPath[T, Const[DeadEnd, ?]] {
      type H[A] = G[A]

      def convertPath[M[_]: Monad](f: ListContents[M]): StaticPathTransformation[T, M, Const[DeadEnd, ?], G] =
        κ(EitherT(List(CoEnv(rootDir[Sandboxed].left[G[T[Pathed[G, ?]]]])).right[FileSystemError].point[M]))
    }

  // NB: This case should be the low-prio default for QScriptTotal components that aren’t Pathable.
  implicit def sourcedPathable[T[_[_]], G[_]] (implicit SP: SourcedPathable[T, ?] :<: G):
      ConvertPath.Aux[T, SourcedPathable[T, ?], G] =
    new ConvertPath[T, SourcedPathable[T, ?]] {
      type H[A] = G[A]

      def convertPath[M[_]: Monad](f: ListContents[M]): StaticPathTransformation[T, M, SourcedPathable[T, ?], G] =
        sp => EitherT(List(CoEnv(SP.inj(sp).right[AbsDir[Sandboxed]])).right.point[M])
    }

  implicit def projectBucket[T[_[_]]: Recursive, G[_]: Functor](
    implicit R: Const[Read, ?] :<: G, PB: ProjectBucket[T, ?] :<: G):
      ConvertPath.Aux[T, ProjectBucket[T, ?], G] =
    new ConvertPath[T, ProjectBucket[T, ?]] {
      type H[A] = G[A]

      def convertPath[M[_]: Monad](f: ListContents[M]): StaticPathTransformation[T, M, ProjectBucket[T, ?], G] = {
        case x @ BucketField(src, _, StrLit(str)) =>
          Recursive[T].project[Pathed[G, ?]](src.copoint).traverse(e => e.run.fold(
            dir => fileType(f).apply(dir, str).map(df => CoEnv(df.map(file => R.inj(Const[Read, T[Pathed[G, ?]]](Read(file)))))).toRight(FileSystemError.pathErr(PathError.invalidPath(dir, "has no entry named “$str”."))),
            κ(EitherT(CoEnv(PB.inj(x).right).right.point[M]))))
        case x => EitherT(List(CoEnv(PB.inj(x).right[AbsDir[Sandboxed]])).right.point[M])
      }
    }

  implicit def coproduct[T[_[_]], F[_], G[_], I[_]](
    implicit F: ConvertPath.Aux[T, F, I], G: ConvertPath.Aux[T, G, I]):
      ConvertPath.Aux[T, Coproduct[F, G, ?], I] =
    new ConvertPath[T, Coproduct[F, G, ?]] {
      type H[A] = I[A]

      def convertPath[M[_]: Monad](f: ListContents[M]): StaticPathTransformation[T, M, Coproduct[F, G, ?], I] =
        _.run.fold(F.convertPath(f), G.convertPath(f))
    }
}
