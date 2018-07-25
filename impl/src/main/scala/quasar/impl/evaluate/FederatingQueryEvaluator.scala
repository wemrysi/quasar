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

package quasar.impl.evaluate

import slamdata.Predef.{None, Option, Some}
import quasar.api.QueryEvaluator
import quasar.api.resource.ResourcePath
import quasar.connector.{MonadResourceErr, ResourceError}
import quasar.contrib.iota.copkTraverse
import quasar.contrib.pathy._
import quasar.contrib.scalaz.MonadTell_
import quasar.fp.PrismNT
import quasar.qscript.{Read => QRead, _}

import matryoshka._
import pathy.Path.refineType
import scalaz._, Scalaz._
import iotaz.CopK

/** A `QueryEvaluator` capable of executing queries against multiple sources. */
final class FederatingQueryEvaluator[
    T[_[_]]: BirecursiveT,
    F[_]: Monad: MonadResourceErr,
    S, R] private (
    queryFederation: QueryFederation[T, F, S, R],
    sources: AFile => F[Option[Source[S]]])
    extends QueryEvaluator[F, T[QScriptEducated[T, ?]], R] {

  // TODO[scalaz]: Shadow the scalaz.Monad.monadMTMAB SI-2712 workaround
  import WriterT.writerTMonadListen

  def evaluate(q: T[QScriptEducated[T, ?]]): F[R] =
    for {
      wa <- Trans.applyTrans(federate, ReadPath)(q).run

      (srcs, qr) = wa

      srcMap = IMap.fromFoldable(srcs)

      fq = FederatedQuery(qr, srcMap.lookup)

      r <- queryFederation.evaluateFederated(fq)
    } yield r

  ////

  private type SrcsT[X[_], A] = WriterT[X, DList[(AFile, Source[S])], A]
  private type M[A] = SrcsT[F, A]

  private val IRD = CopK.Inject[Const[QRead[ADir], ?], QScriptEducated[T, ?]]
  private val IRF = CopK.Inject[Const[QRead[AFile], ?], QScriptEducated[T, ?]]

  private val ReadPath: PrismNT[QScriptEducated[T, ?], Const[QRead[APath], ?]] =
    PrismNT[QScriptEducated[T, ?], Const[QRead[APath], ?]](
      λ[QScriptEducated[T, ?] ~> (Option ∘ Const[QRead[APath], ?])#λ](qr =>
        IRD.prj(qr).map(_.getConst.path)
          .orElse(IRF.prj(qr).map(_.getConst.path))
          .map(p => Const(QRead(p)))),

      λ[Const[QRead[APath], ?] ~> QScriptEducated[T, ?]](rp =>
        refineType(rp.getConst.path).fold(
          d => IRD.inj(Const(QRead(d))),
          f => IRF.inj(Const(QRead(f))))))

  // Record all sources in the query, erroring unless all are known.
  private val federate: Trans[Const[QRead[APath], ?], M] =
    new Trans[Const[QRead[APath], ?], M] {
      def trans[U, G[_]: Functor]
          (GtoF: PrismNT[G, Const[QRead[APath], ?]])
          (implicit UC: Corecursive.Aux[U, G], UR: Recursive.Aux[U, G])
          : Const[QRead[APath], U] => M[G[U]] = {

        case Const(QRead(p)) =>
          val file =
            ResourcePath.leaf.getOption(ResourcePath.fromPath(p)) match {
              case Some(f) =>
                f.point[M]

              case None =>
                MonadResourceErr[M].raiseError(
                  ResourceError.notAResource(ResourcePath.root()))
            }

          for {
            f <- file

            s <- lookupLeaf(f)

            _ <- MonadTell_[M, DList[(AFile, Source[S])]].tell(DList((f, s)))
          } yield GtoF(Const(QRead(f)))
      }
    }

  private def lookupLeaf(file: AFile): M[Source[S]] =
    sources(file).liftM[SrcsT] flatMap {
      case Some(src) =>
        src.point[M]

      case None =>
        MonadResourceErr[M].raiseError(
          ResourceError.pathNotFound(ResourcePath.leaf(file)))
    }
}

object FederatingQueryEvaluator {
  def apply[
      T[_[_]]: BirecursiveT,
      F[_]: Monad: MonadResourceErr,
      S, R](
    queryFederation: QueryFederation[T, F, S, R],
    sources: AFile => F[Option[Source[S]]])
    : QueryEvaluator[F, T[QScriptEducated[T, ?]], R] =
  new FederatingQueryEvaluator(queryFederation, sources)
}
