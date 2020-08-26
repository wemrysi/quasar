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
import quasar.{Condition, ConditionMatchers, EffectfulQSpec}
import quasar.api.resource.ResourcePath

import scala.Predef.assert
import scala.concurrent.ExecutionContext
import java.util.UUID

import cats.effect.{Resource, Effect}
import cats.syntax.apply._
import cats.syntax.flatMap._
import cats.syntax.functor._
import org.specs2.matcher.Matcher
import scalaz.{Equal, IMap, Order, Show, \/, \/-, -\/}
import scalaz.syntax.equal._
import scalaz.syntax.foldable._
import scalaz.std.list._

import shims.{eqToScalaz, equalToCats, showToCats, showToScalaz}

abstract class DatasourcesSpec[
    F[_], G[_], I: Order: Show, C: Equal: Show](
    implicit F: Effect[F], ec: ExecutionContext)
    extends EffectfulQSpec[F]
    with ConditionMatchers {

  import DatasourceError._

  def datasources: Resource[F, Datasources[F, G, I, C]]

  def supportedType: DatasourceType

  // Must be distinct.
  def validConfigs: (C, C)

  def gatherMultiple[A](fga: G[A]): F[List[A]]

  assert(validConfigs._1 =/= validConfigs._2, "validConfigs must be distinct!")

  def mutationExamples(f: (Datasources[F, G, I, C], DatasourceRef[C]) => F[Err \/ I]) = {
    "lookup on success" >>* datasources.use { dses => for {
      a <- refA

      fr <- f(dses, a)

      i <- expectSuccess(fr)

      r <- dses.datasourceRef(i)
    } yield {
      r must beRef(a)
    }}

    "normal status on success" >>* datasources.use { dses => for {
      a <- refA

      fr <- f(dses, a)

      i <- expectSuccess(fr)

      r <- dses.datasourceStatus(i)
    } yield {
      r must be_\/-(beNormal[Exception])
    }}

    "error when name exists" >>* datasources.use { dses => for {
      n <- randomName

      x = DatasourceRef(supportedType, n, validConfigs._1)
      y = DatasourceRef(supportedType, n, validConfigs._2)

      r <- f(dses, x) *> f(dses, y)
    } yield {
      r must be_-\/(equal[Err](DatasourceNameExists(n)))
    }}

    "error when type not supported" >>* {
      val unsup = DatasourceType("--unsupported--", 17)

      datasources.use { dses => for {
        n <- randomName

        uref = DatasourceRef(unsup, n, validConfigs._1)

        u <- dses.addDatasource(uref)

        types <- dses.supportedDatasourceTypes
      } yield {
        u must be_-\/(equal[Err](DatasourceUnsupported(unsup, types)))
      }}
    }
  }

  def resultsInNotFound[E >: ExistentialError[I] <: Err, A](f: (Datasources[F, G, I, C], I) => F[E \/ A]) =
    datasources.use { dses => for {
      i <- refA >>= createRef(dses)

      c <- dses.removeDatasource(i)

      fr <- f(dses, i)
    } yield {
      c must beNormal
      fr must beNotFound[E](i)
    }}

  def discoveryExamples[E >: ExistentialError[I] <: Err, A](f: (Datasources[F, G, I, C], I, ResourcePath) => F[E \/ A]) = {
    "error when datasource not found" >>* {
      resultsInNotFound((dses, i) => f(dses, i, ResourcePath.root()))
    }

    "respond when datasource exists" >>* datasources.use { dses => for {
      i <- refA >>= createRef(dses)

      fr <- f(dses, i, ResourcePath.root())
    } yield {
      fr must not(beNotFound[E](i))
    }}
  }

  "add datasource" >> mutationExamples { (dses, ref) =>
    dses.addDatasource(ref).map(_.leftMap[DatasourceError[I, C]](e => e))
  }


  "replace datasource" >> {
    mutationExamples((dses, ref) => for {
      b <- refB

      i <- createRef(dses)(b)

      c <- dses.replaceDatasource(i, ref)
    } yield {
      Condition.disjunctionIso
        .get(c.map[DatasourceError[I, C]](e => e))
        .map(_ => i)
    })

    "fails when datasource not found" >>* resultsInNotFound { (dses, i) =>
      for {
        b <- refB
        c <- dses.replaceDatasource(i, b)
      } yield {
        Condition.disjunctionIso
          .get(c.map[DatasourceError[I, C]](e => e))
      }
    }
  }

  "all metadata" >> {
    "returns metadata for all datasources" >>* datasources.use { dses => for {
      a <- refA
      ia <- createRef(dses)(a)

      b <- refB
      ib <- createRef(dses)(b)

      g <- dses.allDatasourceMetadata

      ts <- gatherMultiple(g)

      m = IMap.fromFoldable(ts)
    } yield {
      m.lookup(ia).exists(v => v.kind ≟ a.kind && v.name ≟ a.name) must beTrue
      m.lookup(ib).exists(v => v.kind ≟ b.kind && v.name ≟ b.name) must beTrue
    }}
  }

  "lookup ref" >> {
    "error when not found" >>* {
      resultsInNotFound((dses, i) => dses.datasourceRef(i))
    }
  }

  "lookup status" >> {
    "error when not found" >>* {
      resultsInNotFound((dses, i) => dses.datasourceStatus(i))
    }
  }

  "remove datasource" >> {
    "error when not found" >>* {
      resultsInNotFound { (dses, id) =>
        dses.removeDatasource(id)
          .map(Condition.disjunctionIso.get(_))
      }
    }

    "not in metadata on success" >>* datasources.use { dses => for {
      a <- refA
      i <- createRef(dses)(a)

      gBefore <- dses.allDatasourceMetadata
      metaBefore <- gatherMultiple(gBefore)

      _ <- dses.removeDatasource(i)

      gAfter <- dses.allDatasourceMetadata
      metaAfter <- gatherMultiple(gAfter)
    } yield {
      metaBefore.any(_._1 ≟ i) must beTrue
      metaAfter.any(_._1 ≟ i) must beFalse
    }}
  }

  "rename datasource" >> {
    "datasource is renamed when given a new name" >>* datasources.use { dses =>
      for {
        a <- refA
        i <- createRef(dses)(a)
        _ <- dses.renameDatasource(i, DatasourceName("new renamed datasource name"))
        l <- dses.datasourceRef(i)
      } yield {
        l must be_\/-(a.copy(name = DatasourceName("new renamed datasource name")))
      }
    }

    "renaming errors given a name that another datasource has" >>* datasources.use { dses =>
      for {
        a <- refA
        b <- refB
        i <- createRef(dses)(a)
        _ <- createRef(dses)(b)
        rename <- dses.renameDatasource(i, b.name)
        l <- dses.datasourceRef(i)
      } yield {
        rename must beLike {
          case Condition.Abnormal(ex) => ex must_=== DatasourceNameExists(b.name)
        }
        l must be_\/-(a) // datasource was not renamed
      }
    }

    "renaming errors given a nonexisting datasource id" >>* datasources.use { dses =>
      for {
        a <- refA
        i <- createRef(dses)(a)
        _ <- dses.removeDatasource(i)
        rename <- dses.renameDatasource(i, DatasourceName("new renamed datasource name"))
      } yield {
        rename must beLike {
          case Condition.Abnormal(DatasourceNotFound(_)) => ok
        }
      }
    }
  }
  "copy datasource" >> {
    "copies at most renamed datasource located at specified id" >>* datasources.use { dses =>
      for {
        a <- refA
        i0 <- createRef(dses)(prepareForCopy(a))
        i1 <- dses.copyDatasource(i0, _ => DatasourceName("copied"))
        res <- dses.datasourceRef(i1.toOption.get)
      } yield {
        res must beLike {
          case \/-(resDs) =>
            resDs.name must_=== DatasourceName("copied")
            isCopy(a, resDs) must beTrue
        }
      }
    }
    "errors when there is no datasource at specified id" >>* datasources.use { dses =>
      for {
        a <- refA
        i0 <- createRef(dses)(prepareForCopy(a))
        _ <- dses.removeDatasource(i0)
        i1 <- dses.copyDatasource(i0, _ => DatasourceName("copied"))
      } yield {
        i1 must beLike {
          case -\/(DatasourceNotFound(_)) => ok
        }
      }
    }
  }

  ////

  def prepareForCopy(inp: DatasourceRef[C]): DatasourceRef[C] = inp
  def isCopy(source: DatasourceRef[C], copy: DatasourceRef[C]): Boolean =
    DatasourceRef.atMostRenamed(source, copy)

  type Err = DatasourceError[I, C]

  // To allow control over conflicts.
  val randomName: F[DatasourceName] =
    F.delay(DatasourceName("ds-" + UUID.randomUUID()))

  def refA: F[DatasourceRef[C]] =
    randomName map (DatasourceRef(supportedType, _, validConfigs._1))

  def refB: F[DatasourceRef[C]] =
    randomName map (DatasourceRef(supportedType, _, validConfigs._2))

  def sanitizedRefB: F[DatasourceRef[C]] =
    randomName map (DatasourceRef(supportedType, _, validConfigs._2))

  def createRef(dses: Datasources[F, G, I, C]): DatasourceRef[C] => F[I] = r =>
    dses.addDatasource(r) >>= expectSuccess

  def expectSuccess[E <: Err, A](r: E \/ A): F[A] =
    r.fold(
      e => F.raiseError(new RuntimeException(s"Operation failed: ${Show[Err].shows(e)}")),
      F.pure(_))

  def beRef(ref: DatasourceRef[C]): Matcher[ExistentialError[I] \/ DatasourceRef[C]] =
    be_\/-(equal(ref))

  def beNotFound[E >: ExistentialError[I] <: Err](id: I): Matcher[E \/ _] =
    be_-\/[E](equal[Err](DatasourceNotFound(id)) ^^ { (e: E) => e: Err })
}
