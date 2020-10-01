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

package quasar.impl

import slamdata.Predef._

import cats.Show
import cats.effect.{Blocker, ContextShift, Resource, Sync}
import cats.syntax.show._
import cats.instances.string._

import quasar.contrib.scalaz.MonadError_

import java.nio.file.Path
import java.util.UUID
import org.mapdb._
import org.h2.mvstore._

import argonaut.{CodecJson, Parse}
import monocle.Prism

import shims.monadToScalaz

package object storage {
  import IndexedStore._

  val DefaultTableName = "default"

  def errorFromShow[A: Show](desc: String)(a: A): StoreError =
    StoreError.corrupt(s"Failed to decode '$desc' from '${a.show}'")

  def mapDB[F[_]: Sync](path: Path): Resource[F, DB] =
    Resource[F, DB](Sync[F].delay {
      val isWindows: Boolean =
        java.lang.System.getProperty("os.name").toLowerCase.indexOf("windows") > -1

      val rawMaker =
        DBMaker
          .fileDB(path.toFile)
          .checksumHeaderBypass
          .transactionEnable
          .fileLockDisable

      val mmapedMaker =
        if (isWindows)
          rawMaker
        else
          rawMaker
            .fileMmapEnableIfSupported
            .fileMmapPreclearDisable
            .cleanerHackEnable

      val db = mmapedMaker.make

      (db, Sync[F].delay(db.close))
    })

  def inMemoryMapDb[F[_]: Sync]: Resource[F, DB] =
    Resource[F, DB](Sync[F].delay {
      val db = DBMaker.memoryDB().make()
      (db, Sync[F].delay(db.close))
    })

  def mvStore[F[_]: Sync](path: Path): Resource[F, MVStore] =
    Resource[F, MVStore](Sync[F].delay {
      val store = (new MVStore.Builder()).fileName(path.toString).open()
      (store, Sync[F].delay(store.close))
    })

  def offheapMVStore[F[_]: Sync]: Resource[F, MVStore] =
    Resource[F, MVStore](Sync[F].delay {
      val store = (new MVStore.Builder()).fileStore(new OffHeapStore()).open()
      (store, Sync[F].delay(store.close))
    })
}
