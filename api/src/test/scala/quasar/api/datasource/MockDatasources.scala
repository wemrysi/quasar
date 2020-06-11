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

package quasar.api.datasource

import slamdata.Predef._
import quasar.Condition
import quasar.api.datasource.DatasourceError.InitializationError
import quasar.api.resource._
import quasar.contrib.scalaz.MonadState_

import scalaz.{\/, -\/, \/-, ApplicativePlus, IMap, ISet, Monad, Monoid, Tags}
import scalaz.std.anyVal._
import scalaz.syntax.either._
import scalaz.syntax.monad._
import scalaz.syntax.order._
import scalaz.syntax.plusEmpty._
import scalaz.syntax.semigroup._
import scalaz.syntax.std.option._

import MockDatasources.MockState

final class MockDatasources[
    C,
    F[_]: Monad: MonadState_[?[_], MockState[C]],
    G[_]: ApplicativePlus] private (
    supportedTypes: ISet[DatasourceType],
    errorCondition: DatasourceRef[C] => Condition[InitializationError[C]])
    extends Datasources[F, G, Int, C] {

  import DatasourceError._

  type PathType = ResourcePathType

  val mockState = MonadState_[F, MockState[C]]

  def addDatasource(ref: DatasourceRef[C]): F[CreateError[C] \/ Int] =
    for {
      s <- mockState.get

      c <- insertRef[CreateError[C]](s.nextId, ref)

      r <- c match {
        case Condition.Normal() =>
          mockState
            .modify(_.copy(nextId = s.nextId + 1))
            .as(s.nextId.right[CreateError[C]])

        case Condition.Abnormal(e) =>
          e.left[Int].point[F]
      }
    } yield r

  val allDatasourceMetadata: F[G[(Int, DatasourceMeta)]] =
    mockState.gets { case MockState(_, m) =>
      m.foldlWithKey(mempty[G, (Int, DatasourceMeta)])((g, i, t) =>
        g <+> (i, t._1).point[G])
    }

  def datasourceRef(id: Int): F[ExistentialError[Int] \/ DatasourceRef[C]] =
    mockState.gets(
      _.dss.lookup(id)
        .map { case (DatasourceMeta(k, n, _), c) => DatasourceRef(k, n, c) }
        .toRightDisjunction(datasourceNotFound[Int, ExistentialError[Int]](id)))

  def datasourceStatus(id: Int): F[ExistentialError[Int] \/ Condition[Exception]] =
    mockState.gets(
      _.dss.lookup(id)
        .map { case (DatasourceMeta(_, _, c), _) => c }
        .toRightDisjunction(datasourceNotFound[Int, ExistentialError[Int]](id)))

  def replaceDatasource(id: Int, ref: DatasourceRef[C])
      : F[Condition[DatasourceError[Int, C]]] =
    mockState.get flatMap { s =>
      if (s.dss member id)
        insertRef[DatasourceError[Int, C]](id, ref)
      else
        Condition.abnormal(datasourceNotFound[Int, DatasourceError[Int, C]](id)).point[F]
    }

  /* Replaces the datasource ref entirely with the patch.
   */
  def reconfigureDatasource(id: Int, patch: C)
      : F[Condition[DatasourceError[Int, C]]] =
    datasourceRef(id) flatMap {
      case -\/(err) => Condition.abnormal(err: DatasourceError[Int, C]).point[F]
      case \/-(ref) => replaceDatasource(id, ref.copy(config = patch))
    }

  def removeDatasource(id: Int): F[Condition[ExistentialError[Int]]] =
    mockState.get flatMap { s =>
      if (s.dss member id)
        mockState
          .put(s.copy(dss = s.dss.delete(id)))
          .as(Condition.normal[ExistentialError[Int]]())
      else
        Condition.abnormal(datasourceNotFound[Int, ExistentialError[Int]](id)).point[F]
    }

  def renameDatasource(id: Int, name: DatasourceName)
      : F[Condition[DatasourceError[Int, C]]] =
    datasourceRef(id) flatMap {
      case -\/(err) => Condition.abnormal(err: DatasourceError[Int, C]).point[F]
      case \/-(ref) => replaceDatasource(id, ref.copy(name = name))
    }

  val supportedDatasourceTypes: F[ISet[DatasourceType]] = supportedTypes.point[F]

  ////

  private def insertRef[E >: CreateError[C] <: DatasourceError[Int, C]](
      id: Int,
      ref: DatasourceRef[C])
      : F[Condition[E]] =
    if (supportedTypes contains ref.kind)
      mockState.get flatMap { case MockState(nextId, dss) =>
        if (dss.toList.exists(entry => entry._2._1.name === ref.name && entry._1 =/= id))
          Condition.abnormal(datasourceNameExists[E](ref.name)).point[F]
        else
          errorCondition(ref) match {
            case Condition.Normal() =>
              val meta = DatasourceMeta(ref.kind, ref.name, Condition.normal())
              mockState
                .put(MockState(nextId, dss.insert(id, (meta, ref.config))))
                .as(Condition.normal())

            case Condition.Abnormal(e) =>
              Condition.abnormal[E](e).point[F]
          }
      }
    else
      Condition.abnormal(datasourceUnsupported[E](ref.kind, supportedTypes)).point[F]
}

object MockDatasources {
  final case class MockState[C](nextId: Int, dss: IMap[Int, (DatasourceMeta, C)])

  object MockState {
    def empty[C]: MockState[C] =
      MockState(0, IMap.empty)

    implicit def monoid[C]: Monoid[MockState[C]] =
      new Monoid[MockState[C]] {
        import Tags.LastVal

        def append(x: MockState[C], y: => MockState[C]) =
          MockState(
            x.nextId.max(y.nextId),
            LastVal.unsubst(LastVal.subst(x.dss) |+| LastVal.subst(y.dss)))

        val zero = empty
      }
  }

  def apply[
      C,
      F[_]: Monad: MonadState_[?[_], MockState[C]],
      G[_]: ApplicativePlus](
      supportedTypes: ISet[DatasourceType],
      errorCondition: DatasourceRef[C] => Condition[InitializationError[C]])
      : Datasources[F, G, Int, C] =
    new MockDatasources[C, F, G](supportedTypes, errorCondition)
}
