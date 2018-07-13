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

package quasar.evaluate

import slamdata.Predef.{Boolean, List, Option}
import quasar.api._, ResourceError._
import quasar.contrib.pathy._
import quasar.contrib.scalaz.MonadTell_
import quasar.fp.PrismNT
import quasar.contrib.iota.copkTraverse
import quasar.qscript.{Read => QRead, _}

import scala.Predef.implicitly

import matryoshka._
import pathy.Path.refineType
import scalaz._, Scalaz._
import iotaz.CopK

/** A `QueryEvaluator` capable of executing queries against multiple sources. */
final class FederatingQueryEvaluator[T[_[_]]: BirecursiveT, F[_]: Monad, G[_]: ApplicativePlus, S, R] private (
    queryFederation: QueryFederation[T, F, S, R],
    sources: F[IMap[ResourceName, (ResourceDiscovery[F, G], S)]])
    extends QueryEvaluator[F, G, T[QScriptEducated[T, ?]], R] {

  // TODO[scalaz]: Shadow the scalaz.Monad.monadMTMAB SI-2712 workaround
  import WriterT.writerTMonadListen

  def children(path: ResourcePath): F[CommonError \/ G[(ResourceName, ResourcePathType)]] =
    path match {
      case ResourcePath.Root =>
        for {
          rds <- discoveries

          pairs <- rds.foldMapM({ case (n, rd) =>
            rd.isResource(ResourcePath.root()) map { b =>
              (n, b.fold(ResourcePathType.resource, ResourcePathType.resourcePrefix)).point[G]
            }
          })(implicitly, ApplicativePlus[G].monoid)
        } yield pairs.right[CommonError]

      case ResourcePath.Leaf(f) =>
        (for {
          x <- lookupLeaf[CommonError](f)

          (n, d, s) = x

          r <- EitherT(d.children(s.path)) leftMap prefixCommonError(n)
        } yield r).run
    }

  def isResource(path: ResourcePath): F[Boolean] =
    path match {
      case ResourcePath.Root =>
        false.point[F]

      case ResourcePath.Leaf(f) =>
        lookupLeaf[CommonError](f) flatMap {
          case (_, d, s) => EitherT.rightT(d.isResource(s.path))
        } getOrElse false
    }

  def evaluate(q: T[QScriptEducated[T, ?]]): F[ReadError \/ R] =
    (for {
      wa <- Trans.applyTrans(federate, ReadPath)(q).run

      (srcs, qr) = wa

      srcMap = IMap.fromFoldable(srcs)

      fq = FederatedQuery(qr, srcMap.lookup)

      r <- EitherT(queryFederation.evaluateFederated(fq))
    } yield r).run

  ////

  private type SrcsT[X[_], A] = WriterT[X, DList[(AFile, Source[S])], A]
  private type M[A] = SrcsT[EitherT[F, ReadError, ?], A]

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
            EitherT.fromDisjunction[F](
              ResourcePath.leaf
                .getOption(ResourcePath.fromPath(p))
                .toRightDisjunction(notAResource[ReadError](ResourcePath.root())))
              .liftM[SrcsT]

          for {
            f <- file

            l <- lookupLeaf[ReadError](f).liftM[SrcsT]

            _ <- MonadTell_[M, DList[(AFile, Source[S])]].tell(DList((f, l._3)))
          } yield GtoF(Const(QRead(f)))
      }
    }

  private def discoveries: F[List[(ResourceName, ResourceDiscovery[F, G])]] =
    sources.map(_.map(_._1).toAscList)

  private def lookupLeaf[E >: CommonError](file: AFile)
      : EitherT[F, E, (ResourceName, ResourceDiscovery[F, G], Source[S])] = {

    val (n, p) = ResourcePath.unconsLeaf(file)

    OptionT(sources map (_ lookup n))
      .map({ case (rd, s) => (n, rd, Source(p, s)) })
      .toRight(pathNotFound[E](ResourcePath.leaf(file)))
  }

  private def prefixCommonError(pfx: ResourceName): CommonError => CommonError = {
    case PathNotFound(p) => PathNotFound(pfx /: p)
  }
}

object FederatingQueryEvaluator {
  def apply[T[_[_]]: BirecursiveT, F[_]: Monad, G[_]: ApplicativePlus, S, R](
    queryFederation: QueryFederation[T, F, S, R],
    sources: F[IMap[ResourceName, (ResourceDiscovery[F, G], S)]])
    : QueryEvaluator[F, G, T[QScriptEducated[T, ?]], R] =
  new FederatingQueryEvaluator(queryFederation, sources)
}
