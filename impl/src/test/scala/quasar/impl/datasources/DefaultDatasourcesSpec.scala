/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.impl.datasources

import slamdata.Predef.{Stream => _, _}

import quasar.Condition
import quasar.api.MockSchemaConfig
import quasar.api.datasource._
import quasar.api.datasource.DatasourceError._
import quasar.api.resource.{ResourcePath, ResourcePathType}
import quasar.contrib.cats.stateT._
import quasar.contrib.cats.writerT._
import quasar.contrib.cats.effect.stateT.catsStateTEffect
import quasar.contrib.fs2.stream._
import quasar.contrib.scalaz.{MonadState_, MonadError_}
import quasar.impl.storage.PureIndexedStore

import DefaultDatasourcesSpec._

import java.io.IOException
import scala.concurrent.ExecutionContext.Implicits.global

import cats.data.{StateT, WriterT}
import cats.effect.{Resource, IO}
import cats.instances.list._
import cats.syntax.applicative._
import cats.syntax.flatMap._
import eu.timepit.refined.auto._
import fs2.Stream
import matryoshka.data.Fix
import monocle.macros.Lenses
import monocle.{Prism, Lens}
import scalaz.{-\/, IMap, ISet, Monoid, \/-}
import scalaz.std.anyVal._
import scalaz.std.string._
import scalaz.syntax.show._

import shims.{eqToScalaz, equalToCats, monoidToCats, monoidToScalaz, monadToScalaz, showToCats, showToScalaz}

final class DefaultDatasourcesSpec
    extends DatasourcesSpec[DefaultM, Stream[DefaultM, ?], Int, String, MockSchemaConfig.type] {

  import DefaultDatasourcesSpec._
  import MockDatasourceManager._

  type PathType = ResourcePathType

  val monadIdx: MonadState_[DefaultM, Int] =
    MonadState_.zoom[DefaultM](DefaultState.idx)

  implicit val monadDSM: MonadState_[DefaultM, MockDSMState[Int, String]] =
    MonadState_.zoom[DefaultM](dsmLens)

  implicit val monadError: MonadError_[DefaultM, CreateError[String]] =
    MonadError_.facet[DefaultM](createErrorThrowableP)

  implicit val monadRef: MonadState_[DefaultM, Refs] =
    MonadState_.zoom[DefaultM](DefaultState.refs)

  def datasources =
    Resource.pure[DefaultM, Datasources[DefaultM, Stream[DefaultM, ?], Int, String, MockSchemaConfig.type]] {
      mkDatasources(IMap.empty, c => c)(_ => None)
    }
  def dses = mkDatasources(IMap.empty, c => c)(_ => None)
  def sanitizedDatasources = mkDatasources(IMap.empty, _ => "sanitized")(_ => None)

  def supportedType = DatasourceType("test-type", 3L)

  def validConfigs = ("one", "two")

  val schemaConfig = MockSchemaConfig

  def gatherMultiple[A](as: Stream[DefaultM, A]) = as.compile.toList

  def mkDatasources(
      errs: IMap[Int, Exception], sanitize: String => String)(
      init: String => Option[InitializationError[String]])
      : Datasources[DefaultM, Stream[DefaultM, ?], Int, String, MockSchemaConfig.type] = {

    val freshId =
      for {
        i <- monadIdx.get
        _ <- monadIdx.put(i + 1)
      } yield i

    val refs =
      PureIndexedStore[DefaultM, Int, DatasourceRef[String]]

    val errors =
      DatasourceErrors.fromMap(errs.pure[DefaultM])

    val manager =
      MockDatasourceManager[Int, String, Fix, DefaultM, Stream[DefaultM, ?], Unit](
        ISet.singleton(supportedType), init, sanitize, ())

    val schema =
      new ResourceSchema[DefaultM, MockSchemaConfig.type, (ResourcePath, Unit)] {
        def apply(c: MockSchemaConfig.type, r: (ResourcePath, Unit)) =
          MockSchemaConfig.MockSchema.pure[DefaultM]
      }
    DefaultDatasources(freshId, refs, errors, manager, schema)
  }

  "implementation specific" >> {
    "add datasource" >> {
      "initializes datasource" >> {
        val addB =
          refB >>= dses.addDatasource

        addB.runEmpty.value.map(_ must beLike {
          case (s, \/-(i)) => s.cache.member(i) must beTrue
        }).unsafeRunSync()
      }

      "doesn't store config when initialization fails" >> {
        val err3 =
          MalformedConfiguration(supportedType, "three", "3 isn't a config!")

        val ds = mkDatasources(IMap.empty, _ => "") {
          case "three" => Some(err3)
          case _ => None
        }

        val add =
          refA
            .map(DatasourceRef.config.set("three"))
            .flatMap(ds.addDatasource)

        add.runEmpty.value.map(_ must beLike {
          case (s, -\/(e)) =>
            s.refs.isEmpty must beTrue
            s.cache.isEmpty must beTrue
            (e: DatasourceError[Int, String]) must_= err3
        }).unsafeRunSync()
      }
    }

    "lookup status" >> {
      "includes errors" >> {
        val errs =
          IMap[Int, Exception](1 -> new IOException())

        val ds = mkDatasources(errs, _ => "")(_ => None)

        val lbar = for {
          a <- refA
          _ <- ds.addDatasource(a)

          b <- refB
          _ <- ds.addDatasource(b)

          r <- ds.datasourceStatus(1)
        } yield r

        lbar.runEmptyA.value.map(_ must beLike {
          case \/-(Condition.Abnormal(ex)) => ex must beAnInstanceOf[IOException]
        }).unsafeRunSync()
      }
    }

    "all metadata" >> {
      "includes errors" >> {
        val errs =
          IMap[Int, Exception](0 -> new IOException())

        val ds = mkDatasources(errs, _ => "")(_ => None)

        val lbar = for {
          a <- refA
          _ <- ds.addDatasource(a)

          b <- refB
          _ <- ds.addDatasource(b)

          g <- ds.allDatasourceMetadata
          l <- gatherMultiple(g)
          m = IMap.fromList(l)
        } yield m

        lbar.runEmptyA.value.map(_.lookup(0) must beLike {
          case Some(DatasourceMeta(t, _, Condition.Abnormal(ex))) =>
            (t must_= supportedType) and (ex must beAnInstanceOf[IOException])
        }).unsafeRunSync()
      }
    }

    "remove datasource" >> {
      "shutdown existing" >> {
        val sdown = for {
          a <- refA
          _ <- dses.addDatasource(a)
          cond <- dses.removeDatasource(0)
        } yield cond

        sdown.runEmpty.run.map(_ must beLike {
          case (sdowns, (s, Condition.Normal())) =>
            s.cache must_= IMap.empty
            sdowns must_= List(0)
        }).unsafeRunSync()
      }
    }

    "replace datasource" >> {
      "updates manager" >> {
        val replaced = for {
          a <- refA
          b <- refB

          r <- dses.addDatasource(a)
          i = r.toOption.get

          c <- dses.replaceDatasource(i, b)
          res <- dses.pathIsResource(i, ResourcePath.root())
        } yield (b, c)

        replaced.runEmpty.run.map(_ must beLike {
          case (sdowns, (s, (ref, Condition.Normal()))) =>
            s.cache must_= IMap.singleton(0, ref)
            sdowns must_= List(0)
        }).unsafeRunSync()
      }

      "doesn't update manager when only name changed" >> {
        val renamed = for {
          a <- refA
          n <- randomName

          b = DatasourceRef.name.set(n)(a)
          r <- dses.addDatasource(a)
          i = r.toOption.get

          c <- dses.replaceDatasource(i, b)

          res <- dses.pathIsResource(i, ResourcePath.root())
        } yield (a, c)

        renamed.runEmpty.run.map(_ must beLike {
          case (sdowns, (s, (ref, Condition.Normal()))) =>
            s.cache must_= IMap.singleton(0, ref)
            sdowns must_= Nil
        }).unsafeRunSync()
      }
    }

    "sanitize config" >> {
      "ref is sanitized" >> {
        val ds = for {
          a <- refA
          _ <- sanitizedDatasources.addDatasource(a)
          l <- sanitizedDatasources.datasourceRef(0)
        } yield l

        ds.runEmpty.run.map(x => x must beLike {
          case (_, (_, \/-(ref))) => {
            ref.config must_= "sanitized"
          }
        }).unsafeRunSync()
      }
    }
  }
}

object DefaultDatasourcesSpec {
  import MockDatasourceManager._

  type Refs = IMap[Int, DatasourceRef[String]]
  type DefaultM[A] = StateT[WriterT[IO, Shutdowns[Int], ?], DefaultState, A]

  private final case class CreateException(err: CreateError[String]) extends Exception(err.shows)

  val createErrorThrowableP: Prism[Throwable, CreateError[String]] =
    Prism.partial[Throwable, CreateError[String]] {
      case CreateException(err) => err
    } (CreateException(_))

  val dsmLens: Lens[DefaultState, MockDSMState[Int, String]] =
    Lens[DefaultState, MockDSMState[Int, String]](
      (x: DefaultState) => MockDSMState(x.cache, x.refs))( mdst => st =>
        st.copy(cache = mdst.cache, refs = mdst.refs))


  @Lenses
  final case class DefaultState(idx: Int, cache: Refs, refs: Refs)

  object DefaultState {
    implicit val monoid: Monoid[DefaultState] =
      new Monoid[DefaultState] {
        val zero = DefaultState(0, IMap.empty, IMap.empty)

        def append(x: DefaultState, y: => DefaultState) =
          DefaultState(
            x.idx + y.idx,
            x.cache union y.cache,
            x.refs union y.refs)
      }
  }
}
