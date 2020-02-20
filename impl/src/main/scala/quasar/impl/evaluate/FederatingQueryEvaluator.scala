/*
 * Copyright 2014–2020 SlamData Inc.
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

package quasar.impl.evaluate

import slamdata.Predef.{None, Option, Some}

import quasar.api.QueryEvaluator
import quasar.api.resource.ResourcePath
import quasar.connector.{MonadResourceErr, ResourceError}
import quasar.connector.evaluate._
import quasar.contrib.cats.writerT._
import quasar.contrib.iota.copkTraverse
import quasar.contrib.pathy._
import quasar.contrib.scalaz.MonadTell_
import quasar.fp.PrismNT
import quasar.qscript.{Read => QRead, _}

import scala.collection.immutable.SortedMap

import iotaz.CopK

import matryoshka._

import cats.{Monad, Order}
import cats.data.{Chain, WriterT}
import cats.syntax.applicative._
import cats.syntax.flatMap._
import cats.syntax.functor._

import scalaz.{~>, Const}

import shims.{orderToCats, monadToScalaz}

object FederatingQueryEvaluator {
  def apply[T[_[_]]: BirecursiveT, F[_]: Monad: MonadResourceErr, S, R](
      queryFederation: QueryFederation[T, F, S, R],
      sources: AFile => F[Option[Source[S]]])
      : QueryEvaluator[F, T[QScriptEducated[T, ?]], R] =
    QueryEvaluator(new FederatingQueryEvaluatorImpl(queryFederation, sources))
}

/** A `QueryEvaluator` capable of executing queries against multiple sources. */
private[evaluate] final class FederatingQueryEvaluatorImpl[
    T[_[_]]: BirecursiveT,
    F[_]: Monad: MonadResourceErr,
    S, R](
    queryFederation: QueryFederation[T, F, S, R],
    sources: AFile => F[Option[Source[S]]])
    extends (T[QScriptEducated[T, ?]] => F[R]) {

  def apply(q: T[QScriptEducated[T, ?]]): F[R] =
    for {
      wa <- Trans.applyTrans(federate, ReadPath)(q).run

      (srcs, qr) = wa

      srcMap = srcs.foldLeft(SortedMap.empty[AFile, Source[S]](Order[AFile].toOrdering))(_ + _)

      fq = FederatedQuery(qr, srcMap.get)

      r <- queryFederation(fq)
    } yield r

  ////

  private type W = Chain[(AFile, Source[S])]
  private type SrcsT[X[_], A] = WriterT[X, W, A]
  private type M[A] = SrcsT[F, A]

  private val QR = CopK.Inject[Const[QRead[ResourcePath], ?], QScriptEducated[T, ?]]

  private val ReadPath: PrismNT[QScriptEducated[T, ?], Const[QRead[ResourcePath], ?]] =
    PrismNT[QScriptEducated[T, ?], Const[QRead[ResourcePath], ?]](
      λ[QScriptEducated[T, ?] ~> (Option ∘ Const[QRead[ResourcePath], ?])#λ](
        qr => QR.prj(qr)),

      λ[Const[QRead[ResourcePath], ?] ~> QScriptEducated[T, ?]](
        rp => QR.inj(rp)))

  // Record all sources in the query, erroring unless all are known.
  private val federate: Trans[Const[QRead[ResourcePath], ?], M] =
    new Trans[Const[QRead[ResourcePath], ?], M] {
      def trans[U, G[_]: scalaz.Functor]
          (GtoF: PrismNT[G, Const[QRead[ResourcePath], ?]])
          (implicit UC: Corecursive.Aux[U, G], UR: Recursive.Aux[U, G])
          : Const[QRead[ResourcePath], U] => M[G[U]] = {

        case Const(QRead(path, idStatus)) =>
          val sourceM: M[(AFile, Source[S])] = path match {
            case ResourcePath.Leaf(file) =>
              lookupLeaf(file).map(s => (file, s))

            case root @ ResourcePath.Root =>
              MonadResourceErr[M].raiseError(
                ResourceError.notAResource(root))
          }

          for {
            source <- sourceM
            _ <- MonadTell_[M, W].tell(Chain.one(source))
          } yield GtoF(Const(QRead(path, idStatus)))
      }
    }

  private def lookupLeaf(file: AFile): M[Source[S]] =
    WriterT.liftF[F, W, Option[Source[S]]](sources(file)) flatMap {
      case Some(src) =>
        src.pure[M]

      case None =>
        MonadResourceErr[M].raiseError(
          ResourceError.pathNotFound(ResourcePath.leaf(file)))
    }
}
