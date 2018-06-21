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

package quasar.impl.datasources

import slamdata.Predef._
import quasar.Qspec
import quasar.api.{ResourceName, ResourceNameGenerator}

import org.scalacheck.Arbitrary
import org.specs2.scalacheck._
import scalaz.{~>, Equal, Id, IMap, Monad, Show}, Id.Id
import scalaz.std.option._
import scalaz.syntax.equal._
import scalaz.syntax.monad._

abstract class DataSourceConfigsSpec[F[_]: Monad, C: Arbitrary: Equal: Show]
    extends Qspec
    with ResourceNameGenerator
    with DataSourceConfigGenerator {

  def dataSourceConfigs: F[DataSourceConfigs[F, C]]

  def run: F ~> Id

  // These are side-effecting, so only run a few.
  implicit val params = Parameters(minTestsOk = 5)

  def withDataSourceConfigs[A](f: DataSourceConfigs[F, C] => F[A]): A =
    run(dataSourceConfigs flatMap f)

  "add" >> {
    "creates new config" >> prop { (n: ResourceName, c: DataSourceConfig[C]) =>
      withDataSourceConfigs(cfgs => for {
        pre <- cfgs.lookup(n)
        _ <- cfgs.add(n, c)
        post <- cfgs.lookup(n)
      } yield {
        pre must beNone
        post must_= Some(c)
      })
    }

    "replaces existing config" >> prop {
      (n: ResourceName, c1: DataSourceConfig[C], c2: DataSourceConfig[C]) => (c1 =/= c2) ==> {

      withDataSourceConfigs(cfgs => for {
        _ <- cfgs.add(n, c1)
        _ <- cfgs.add(n, c2)
        r <- cfgs.lookup(n)
      } yield r must_= Some(c2))
    }}
  }

  "configured" >> {
    "empty when no configs" >> {
      withDataSourceConfigs(_.configured map (_ must_= IMap.empty))
    }

    "contains all existing configurations" >> prop {
      (n1: ResourceName, n2: ResourceName, c1: DataSourceConfig[C], c2: DataSourceConfig[C]) => (n1 =/= n2) ==> {

      withDataSourceConfigs(cfgs => for {
        _ <- cfgs.add(n1, c1)
        _ <- cfgs.add(n2, c2)
        expect = IMap(n1 -> c1.kind, n2 -> c2.kind)
        meta <- cfgs.configured
      } yield meta must_= expect)
    }}
  }

  "remove" >> {
    "false when name doesn't exist" >> prop { (n: ResourceName) =>
      withDataSourceConfigs(_.remove(n) map (_ must beFalse))
    }

    "true and deletes config when exists" >> prop { (n: ResourceName, c: DataSourceConfig[C]) =>
      withDataSourceConfigs(cfgs => for {
        _ <- cfgs.add(n, c)
        pre <- cfgs.lookup(n)
        r <- cfgs.remove(n)
        post <- cfgs.lookup(n)
      } yield {
        pre must_= Some(c)
        r must beTrue
        post must beNone
      })
    }
  }

  "rename" >> {
    "nonexistent src is a no-op" >> prop { (n1: ResourceName, n2: ResourceName, c: DataSourceConfig[C]) => (n1 =/= n2) ==> {
      withDataSourceConfigs(cfgs => for {
        _ <- cfgs.add(n1, c)
        _ <- cfgs.rename(n2, n1)
        l1 <- cfgs.lookup(n1)
        l2 <- cfgs.lookup(n2)
      } yield {
        l1 must_= Some(c)
        l2 must beNone
      })
    }}

    "makes src available at dst and no longer available at src" >> prop {
      (n1: ResourceName, n2: ResourceName, c: DataSourceConfig[C]) => (n1 =/= n2) ==> {

      withDataSourceConfigs(cfgs => for {
        _ <- cfgs.add(n1, c)
        pre <- cfgs.lookup(n1)
        _ <- cfgs.rename(n1, n2)
        post1 <- cfgs.lookup(n1)
        post2 <- cfgs.lookup(n2)
      } yield {
        pre must_= Some(c)
        post1 must beNone
        post2 must_= Some(c)
      })
    }}

    "overwrites any existing dst" >> prop {
      (n1: ResourceName, n2: ResourceName, c1: DataSourceConfig[C], c2: DataSourceConfig[C]) => (n1 =/= n2 && c1 =/= c2) ==> {

      withDataSourceConfigs(cfgs => for {
        _ <- cfgs.add(n1, c1)
        _ <- cfgs.add(n2, c2)
        pre1 <- cfgs.lookup(n1)
        pre2 <- cfgs.lookup(n2)
        _ <- cfgs.rename(n1, n2)
        post1 <- cfgs.lookup(n1)
        post2 <- cfgs.lookup(n2)
      } yield {
        pre1 must_= Some(c1)
        pre2 must_= Some(c2)
        post1 must beNone
        post2 must_= Some(c1)
      })
    }}
  }
}
