/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.main.api

import quasar.contrib.scalaz._
import quasar.fs.mount.MountConfig
import quasar.main._
import quasar.metastore.MetaStoreFixture
import quasar.sql._

import scala.StringContext

import pathy.Path._
import scalaz._, Scalaz._

class ApiSpec extends quasar.Qspec {
  "Api" >> {
    "change metastore at runtime" >> {
      val sampleMountPath = rootDir </> file("foo")
      val sampleMount = MountConfig.viewConfig0(sqlB"select * from zips")
      (for {
        firstMetaConf <- MetaStoreFixture.createNewTestMetaStoreConfig
        quasarFS      <- Quasar.initWithDbConfig(firstMetaConf).run.map(a => a.leftMap(e => new scala.Exception(e.shows))).unattempt
        _             <- quasarFS.mount(sampleMountPath, sampleMount, false)
        mountIsThere  <- quasarFS.getMount(sampleMountPath).map(_.isDefined)
        otherMetaConf <- MetaStoreFixture.createNewTestMetaStoreConfig
        _             <- quasarFS.attemptChangeMetastore(otherMetaConf, initialize = true)
        noLongerThere <- quasarFS.getMount(sampleMountPath).map(_.empty)
        _             <- quasarFS.attemptChangeMetastore(firstMetaConf, initialize = true)
        thereAgain    <- quasarFS.getMount(sampleMountPath).map(_.isDefined)
      } yield {
        mountIsThere  must_=== true
        noLongerThere must_=== true
        thereAgain    must_=== true
      }).unsafePerformSync
    }
  }
}