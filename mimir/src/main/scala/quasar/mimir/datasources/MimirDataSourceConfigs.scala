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

package quasar.mimir.datasources

import slamdata.Predef.{Boolean, Map, Option, Unit}

import quasar.api.{DataSourceType, ResourceName}
import quasar.blueeyes.json.{JValue, JUndefined}
import quasar.contrib.fs2.convert
import quasar.contrib.pathy.AFile
import quasar.contrib.scalaz.MonadError_
import quasar.fp.numeric.Positive
import quasar.impl.datasources.{DataSourceConfig, DataSourceConfigs}
import quasar.mimir.MimirCake.Cake
import quasar.mimir.slicesToStream
import quasar.precog.common.{Path => PrecogPath, _}
import quasar.yggdrasil.bytecode.{JObjectFixedT, JType, JTextT, JNumberT}
import quasar.yggdrasil.table.Slice
import quasar.yggdrasil.vfs.ResourceError

import cats.effect.{IO, LiftIO}
import eu.timepit.refined.refineV
import fs2.Stream
import monocle.Prism
import pathy.Path._
import scalaz.{\/, -\/, \/-, EitherT, IMap, Monad, NonEmptyList, Scalaz, StreamT}, Scalaz._
import shims.monadToScalaz

final class MimirDataSourceConfigs[
    F[_]: LiftIO: Monad] private (
    precog: Cake,
    tableLoc: AFile)(
    implicit ME: MonadError_[F, ResourceError])
    extends DataSourceConfigs[F, RValue] {

  import MimirDataSourceConfigs._
  import precog.trans._
  import precog.Library.Infix

  def add(name: ResourceName, config: DataSourceConfig[RValue]): F[Unit] = {
    val namedCfg =
      RValue.rField(NameField)
        .set(Some(CString(name.value)))
        .apply(dataSourceConfigP(config))

    absorbError(for {
      noName <- without(name.wrapNel)

      added = noName ++ Stream(namedCfg.toJValue)

      _ <- precog.ingest(tablePath, added)
    } yield ())
  }

  def configured: F[IMap[ResourceName, DataSourceType]] = {
    def decodeMeta(v: RValue): ResourceError \/ (ResourceName, DataSourceType) = {
      val maybeMeta = for {
        name <-
          RValue.rField1(NameField)
            .composePrism(RValue.rString)
            .getOption(v)

        typeNameS <-
          RValue.rField1(TypeField)
            .composePrism(RValue.rString)
            .getOption(v)

        typeName <- refineV[DataSourceType.NameP](typeNameS).toOption

        verN <-
          RValue.rField1(TypeVersionField)
            .composePrism(RValue.rNum)
            .getOption(v)

        ver <- Positive(verN.longValue)
      } yield (ResourceName(name), DataSourceType(typeName, ver))

      maybeMeta \/> ResourceError.corrupt("Failed to decode config metadata from: " + v)
    }

    val rvalues =
      absorbError(loadValues(MetaType) flatMap { t =>
        EitherT.rightT(slicesValues(t.slices))
      })

    ME.unattempt(
      rvalues map { rvs =>
        rvs.foldLeftM(IMap.empty[ResourceName, DataSourceType]) { (m, rv) =>
          decodeMeta(rv) map (m + _)
        }
      })
  }

  def lookup(name: ResourceName): F[Option[DataSourceConfig[RValue]]] =
    for {
      rval <- absorbError(lookupValue(name))

      cfg <- rval traverse { v =>
        ME.unattempt_(
          dataSourceConfigP
            .getOption(v)
            .toRightDisjunction(ResourceError.corrupt("Malformed data source config: " + v)))
      }
    } yield cfg

  def remove(name: ResourceName): F[Boolean] =
    absorbError(for {
      rval <- lookupValue(name)

      existed = rval.isDefined

      _ <- existed.whenM(without(name.wrapNel) >>= (precog.ingest(tablePath, _)))
    } yield existed)

  def rename(src: ResourceName, dst: ResourceName): F[Unit] =
    absorbError(for {
      rval <- lookupValue(src)

      updated = rval.map(RValue.rField1(NameField).set(CString(dst.value)))

      _ <- updated traverse_ { upd =>
        without(NonEmptyList(src, dst)) flatMap { s =>
          precog.ingest(tablePath, s ++ Stream(upd.toJValue))
        }
      }
    } yield ())

  ////

  private val tablePath: PrecogPath =
    PrecogPath(posixCodec.printPath(tableLoc))

  private def absorbError[A](fa: EitherT[IO, ResourceError, A]): F[A] =
    ME.unattempt(fa.run.to[F])

  private def lookupValue(name: ResourceName): EitherT[IO, ResourceError, Option[RValue]] = {
    val tspec =
      EqualLiteral(
        DerefObjectStatic(TransSpec1.Id, CPathField(NameField)),
        CString(name.value),
        false)

    for {
      t <- loadValues(JType.JUniverseT)

      filtered = t.transform(Filter(TransSpec1.Id, tspec))

      rvals <- EitherT.rightT(slicesValues(filtered.slices))
    } yield rvals.headOption
  }

  private def loadValues(shape: JType): EitherT[IO, ResourceError, precog.Table] =
    MimirDataSourceConfigs.loadValues(precog, tableLoc, shape)

  private def slicesValues(slices: StreamT[IO, Slice]): IO[List[RValue]] =
    slices.foldLeft(List[RValue]())((v, s) => s.toRValues ::: v)

  private def without(names: NonEmptyList[ResourceName]): EitherT[IO, ResourceError, Stream[IO, JValue]] = {
    def neq(n: ResourceName): TransSpec[Source1] =
      EqualLiteral(
        DerefObjectStatic(TransSpec1.Id, CPathField(NameField)),
        CString(n.value),
        true)

    val tspec =
      names.foldMapRight1(neq)((n, ts) => Infix.And.spec2(neq(n), ts))

    loadValues(JType.JUniverseT) map { t =>
      slicesToStream(t.transform(Filter(TransSpec1.Id, tspec)).slices)
        .filter(_ =/= JUndefined)
    }
  }
}

object MimirDataSourceConfigs {
  val NameField = "name"
  val TypeField = "type"
  val TypeVersionField = "type_version"
  val ConfigField = "config"

  val MetaType: JType =
    JObjectFixedT(Map(
      NameField -> JTextT,
      TypeField -> JTextT,
      TypeVersionField -> JNumberT))

  def apply[F[_]: LiftIO: Monad: MonadError_[?[_], ResourceError]](
      precog: Cake,
      tableLoc: AFile)
      : DataSourceConfigs[F, RValue] =
    new MimirDataSourceConfigs[F](precog, tableLoc)

  val dataSourceConfigP: Prism[RValue, DataSourceConfig[RValue]] = {
    def fromRValue(rv: RValue): Option[DataSourceConfig[RValue]] =
      for {
        name <-
          RValue.rField1(TypeField)
            .composePrism(RValue.rString)
            .getOption(rv)

        ver <-
          RValue.rField1(TypeVersionField)
            .composePrism(RValue.rNum)
            .getOption(rv)

        cfgV <- RValue.rField1(ConfigField).getOption(rv)

        cfg <- cfgV match {
          case CUndefined => None
          case other => Some(other)
        }

        tname <- refineV[DataSourceType.NameP](name).toOption

        tver <- Positive(ver.longValue)
      } yield DataSourceConfig(DataSourceType(tname, tver), cfg)

    val toRValue: DataSourceConfig[RValue] => RValue = {
      case DataSourceConfig(DataSourceType(t, v), c) =>
        RValue.rObject(Map(
          TypeField -> CString(t.value),
          TypeVersionField -> CNum(v.value),
          ConfigField -> c))
    }

    Prism(fromRValue)(toRValue)
  }

  // TODO{logging}: Log any values we weren't able to deserialize properly
  def allConfigs(precog: Cake, tableLoc: AFile)
      : EitherT[IO, ResourceError, Stream[IO, (ResourceName, DataSourceConfig[RValue])]] =
    loadValues(precog, tableLoc, JType.JUniverseT) map { t =>
      val pairs = for {
        slice <- convert.fromStreamT(t.slices)

        rvalue <- Stream.emits(slice.toRValues)

        maybePair = for {
          name <-
            RValue.rField1(NameField)
              .composePrism(RValue.rString)
              .getOption(rvalue)

          cfg <- dataSourceConfigP.getOption(rvalue)
        } yield (ResourceName(name), cfg)
      } yield maybePair

      pairs.unNone
    }

  def loadValues(precog: Cake, tableLoc: AFile, shape: JType)
      : EitherT[IO, ResourceError, precog.Table] = {

    import precog.trans.constants._

    val source = Set(posixCodec.printPath(tableLoc))

    EitherT(precog.Table.constString(source).load(shape).run map {
      case \/-(t) =>
        \/-(t.transform(SourceValue.Single))

      case -\/(ResourceError.NotFound(_)) =>
        \/-(precog.Table.empty)

      case -\/(err) => -\/(err)
    })
  }
}
