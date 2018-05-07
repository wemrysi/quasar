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

package quasar

import slamdata.Predef.{List, Option}
import quasar.api._, ResourceError._
import quasar.contrib.pathy._
import quasar.qscript._

import matryoshka._
import matryoshka.implicits._
import scalaz.{Const, EitherT, IMap, Inject, Monad, OptionT, Tree}
import scalaz.Scalaz._

/** A `QueryEvaluator` capable of executing queries against multiple sources. */
final class FederatingQueryEvaluator[T[_[_]]: BirecursiveT, F[_]: Monad, S, R] private (
    queryFederation: QueryFederation[T, F, S, R],
    sources: F[IMap[ResourceName, (ResourceDiscovery[F], S)]])
    extends QueryEvaluator[F, T[QScriptRead[T, ?]], R] {

  def children(path: ResourcePath) =
    path match {
      case ResourcePath.Root =>
        for {
          rds <- discoveries

          pairs <- rds.traverse { case (n, rd) =>
            rd.isResource(ResourcePath.root()) map { b =>
              (n, b.fold(ResourcePathType.resource, ResourcePathType.resourcePrefix))
            }
          }
        } yield IMap.fromFoldable(pairs).right[CommonError]

      case ResourcePath.Leaf(f) =>
        (for {
          x <- lookupLeaf[CommonError](f)

          (p, d, s) = x

          r <- EitherT(d.children(p)) leftMap prefixCommonError(s.name)
        } yield r).run
    }

  def descendants(path: ResourcePath) =
    (path match {
      case ResourcePath.Root =>
        for {
          rds <- discoveries.liftM[EitherT[?[_], CommonError, ?]]

          subTrees <- rds.traverse {
            case (n, rd) =>
              EitherT(rd.descendants(ResourcePath.root())) map (Tree.Node(n, _))
          }
        } yield subTrees.toStream

      case ResourcePath.Leaf(f) =>
        for {
          x <- lookupLeaf[CommonError](f)

          (p, d, s) = x

          r <- EitherT(d.descendants(p)) leftMap prefixCommonError(s.name)
        } yield r
    }).run

  def isResource(path: ResourcePath) =
    path match {
      case ResourcePath.Root =>
        false.point[F]

      case ResourcePath.Leaf(f) =>
        lookupLeaf[CommonError](f) flatMap {
          case (p, d, _) => EitherT.rightT(d.isResource(p))
        } getOrElse false
    }

  def evaluate(q: T[QScriptRead[T, ?]]) = {
    type G[A] = EitherT[F, ReadError, A]

    val qsFederated = q.transCataM[G, T[QScriptFederated[T, S, ?]], QScriptFederated[T, S, ?]] {
      case ReadPath(p) =>
        val rp = ResourcePath.fromPath(p)

        for {
          f <- EitherT.fromDisjunction[F] {
            ResourcePath.leaf.getOption(rp) \/> notAResource[ReadError](ResourcePath.root())
          }

          l <- lookupLeaf[ReadError](f)

          (rp, _, s) = l

        } yield QScriptFederated.RD(rp, s)

      case RQC(qc) =>
        QScriptFederated.QC(qc).point[G]

      case RTJ(tj) =>
        QScriptFederated.TJ(tj).point[G]
    }

    qsFederated.flatMapF(queryFederation.evaluateFederated).run
  }

  ////

  private object ReadPath {
    val RD = Inject[Const[Read[ADir], ?], QScriptRead[T, ?]]
    val RF = Inject[Const[Read[AFile], ?], QScriptRead[T, ?]]

    def unapply[A](qr: QScriptRead[T, A]): Option[APath] =
      RD.prj(qr).map(_.getConst.path) orElse RF.prj(qr).map(_.getConst.path)
  }

  private object RQC {
    def unapply[A](qr: QScriptRead[T, A]): Option[QScriptCore[T, A]] =
      Inject[QScriptCore[T, ?], QScriptRead[T, ?]].prj(qr)
  }

  private object RTJ {
    def unapply[A](qr: QScriptRead[T, A]): Option[ThetaJoin[T, A]] =
      Inject[ThetaJoin[T, ?], QScriptRead[T, ?]].prj(qr)
  }

  private def discoveries: F[List[(ResourceName, ResourceDiscovery[F])]] =
    sources.map(_.map(_._1).toAscList)

  private def lookupLeaf[E >: CommonError](file: AFile)
      : EitherT[F, E, (ResourcePath, ResourceDiscovery[F], Source[S])] = {

    val (n, p) = ResourcePath.unconsLeaf(file)

    OptionT(sources map (_ lookup n))
      .map({ case (rd, s) => (p, rd, Source(n, s)) })
      .toRight(pathNotFound[E](ResourcePath.leaf(file)))
  }

  private def prefixCommonError(pfx: ResourceName): CommonError => CommonError = {
    case PathNotFound(p) => PathNotFound(pfx /: p)
  }
}

object FederatingQueryEvaluator {
  def apply[T[_[_]]: BirecursiveT, F[_]: Monad, S, R](
    queryFederation: QueryFederation[T, F, S, R],
    sources: F[IMap[ResourceName, (ResourceDiscovery[F], S)]])
    : QueryEvaluator[F, T[QScriptRead[T, ?]], R] =
  new FederatingQueryEvaluator(queryFederation, sources)
}
