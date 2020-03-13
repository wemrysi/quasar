/*
 * Copyright 2020 Precog Data
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

import quasar.api.resource.{ResourcePath, ResourcePathType}
import quasar.connector.{MonadResourceErr, Offset, ResourceError}
import quasar.connector.datasource.{BatchLoader, Loader}
import quasar.connector.evaluate._
import quasar.contrib.cats.writerT._
import quasar.contrib.iota.copkTraverse
import quasar.contrib.pathy._
import quasar.contrib.scalaz.MonadTell_
import quasar.fp.PrismNT
import quasar.impl.QuasarDatasource
import quasar.qscript.{Read => QRead, _}

import iotaz.CopK

import matryoshka._

import cats.{Monad, Order, Traverse}
import cats.data.{Chain, Kleisli, NonEmptyList, WriterT}
import cats.implicits._

import scala.collection.immutable.SortedMap

import scalaz.{~>, Const}

import shims.{orderToCats, monadToScalaz}

/** Maps resource references to `QueryAssociate`s, validating they are able to fulfill the
  * request given their current capabilities.
  */
object QueryFederator {
  def apply[T[_[_]]: BirecursiveT, F[_]: Monad: MonadResourceErr, G[_], R, P <: ResourcePathType](
      sources: AFile => F[Option[Source[QuasarDatasource[T, F, G, R, P]]]])
      : Kleisli[F, (T[QScriptEducated[T, ?]], Option[Offset]), FederatedQuery[T, QueryAssociate[T, F, R]]] =
    Kleisli(new QueryFederatorImpl(sources).tupled)
}

private[evaluate] final class QueryFederatorImpl[
    T[_[_]]: BirecursiveT,
    F[_]: Monad: MonadResourceErr,
    G[_],
    R, P <: ResourcePathType](
    sources: AFile => F[Option[Source[QuasarDatasource[T, F, G, R, P]]]])
    extends ((T[QScriptEducated[T, ?]], Option[Offset]) => F[FederatedQuery[T, QueryAssociate[T, F, R]]]) {

  def apply(query: T[QScriptEducated[T, ?]], offset: Option[Offset])
      : F[FederatedQuery[T, QueryAssociate[T, F, R]]] =
    for {
      (srcs, qr) <- Trans.applyTrans(federate, ReadPath)(query).run

      srcMap = srcs.foldLeft(SortedMap.empty[AFile, Src](Order[AFile].toOrdering))(_ + _)

      assocs <- extractAssocs(srcMap, offset)

    } yield FederatedQuery(qr, assocs.get)

  ////

  private type Src = Source[QuasarDatasource[T, F, G, R, P]]
  private type Srcs = Chain[(AFile, Src)]
  private type SrcsT[X[_], A] = WriterT[X, Srcs, A]
  private type M[A] = SrcsT[F, A]

  private val TMS = Traverse[SortedMap[AFile, ?]].compose[Source]
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
          val sourceM: M[(AFile, Src)] = path match {
            case ResourcePath.Leaf(file) =>
              lookupLeaf(file).map(s => (file, s))

            case root @ ResourcePath.Root =>
              MonadResourceErr[M].raiseError(
                ResourceError.notAResource(root))
          }

          for {
            source <- sourceM
            _ <- MonadTell_[M, Srcs].tell(Chain.one(source))
          } yield GtoF(Const(QRead(path, idStatus)))
      }
    }

  private def extractAssocs(srcs: SortedMap[AFile, Src], offset0: Option[Offset])
      : F[SortedMap[AFile, Source[QueryAssociate[T, F, R]]]] = {

    def orElseSeekUnsupported[A](file: AFile, a: Option[A]): F[A] =
      a match {
        case None =>
          MonadResourceErr[F].raiseError[A](
            ResourceError.seekUnsupported(ResourcePath.leaf(file)))

        case Some(a) => a.pure[F]
      }

    offset0 match {
      case Some(_) if srcs.size > 1 =>
        MonadResourceErr[F].raiseError(ResourceError.tooManyResources(
          NonEmptyList.fromListUnsafe(srcs.keys.map(ResourcePath.leaf(_)).toList),
          "Offset queries require a single resource."))

      case offset @ Some(_) =>
        TMS.traverse(srcs.transform((f, s) => s.tupleLeft(f))) {
          case (path, QuasarDatasource.Lightweight(lw)) =>
            orElseSeekUnsupported(path, lw.loaders.toList collectFirst {
              case Loader.Batch(BatchLoader.Seek(f)) =>
                QueryAssociate.lightweight[T](f(_, offset))
            })

          case (path, QuasarDatasource.Heavyweight(hw)) =>
            orElseSeekUnsupported(path, hw.loaders.toList collectFirst {
              case Loader.Batch(BatchLoader.Seek(f)) =>
                QueryAssociate.heavyweight(f(_, offset))
            })
        }

      case None =>
        Monad[F].pure(TMS.map(srcs) {
          case QuasarDatasource.Lightweight(lw) =>
            lw.loaders.head match {
              case Loader.Batch(b) => QueryAssociate.lightweight[T](b.loadFull)
            }

          case QuasarDatasource.Heavyweight(hw) =>
            hw.loaders.head match {
              case Loader.Batch(b) => QueryAssociate.heavyweight(b.loadFull)
            }
        })
    }
  }

  private def lookupLeaf(file: AFile): M[Src] =
    WriterT.liftF[F, Srcs, Option[Src]](sources(file)) flatMap {
      case Some(src) =>
        src.pure[M]

      case None =>
        MonadResourceErr[M].raiseError(
          ResourceError.pathNotFound(ResourcePath.leaf(file)))
    }
}
