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

package quasar.impl.storage

import slamdata.Predef._

import cats.effect.Sync
import cats.syntax.flatMap._
import cats.syntax.apply._
import fs2.Stream
import shims._

import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.mapdb.{DBMaker, DB, BTreeMap, Serializer}

final class MapDBIndexedStore[F[_]: Sync] private(db: DB) extends IndexedStore[F, String, String] {
  import java.util.{Map => JMap}
  import MapDBIndexedStore._

  def getMap: F[BTreeMap[String, String]] = Sync[F].delay {
    db.treeMap(TREEMAP_NAME).keySerializer(Serializer.STRING).valueSerializer(Serializer.STRING).createOrOpen
  }

  def entries: Stream[F, (String, String)] = for {
    bmap <- Stream.eval(getMap)
    iterator <- Stream.eval(Sync[F].delay(bmap.entryIterator.asScala))
    entry <- Stream.fromIterator[F, JMap.Entry[String, String]](iterator)
  } yield (entry.getKey, entry.getValue)

  def lookup(k: String): F[Option[String]] = getMap flatMap { bmap => Sync[F].delay {
    Option(bmap get k)
  }}

  def insert(k: String, v: String): F[Unit] = getMap flatMap { (bmap: BTreeMap[String, String]) => Sync[F].delay {
    bmap.put(k, v)
  }} productR commit

  def delete(k: String): F[Boolean] = getMap flatMap { bmap => Sync[F].delay {
    !Option(bmap.remove(k)).isEmpty
  }}

  private def commit: F[Unit] = Sync[F].delay(db.commit)

}
object MapDBIndexedStore {
  val TREEMAP_NAME: String = "main"

  private val cache: ConcurrentHashMap[String, DB] = new ConcurrentHashMap()

  private val isWindows: Boolean =
    java.lang.System.getProperty("os.name").toLowerCase.indexOf("windows") > -1

  def apply[F[_]: Sync](path: Path): F[IndexedStore[F, String, String]] =
    makeOrGetDB[F](path) flatMap { db => Sync[F].delay(new MapDBIndexedStore[F](db)) }

  private def mkDb(path: Path): DB = {
    val rawMaker = DBMaker.fileDB(path.toFile).checksumHeaderBypass.transactionEnable.closeOnJvmShutdown.fileLockDisable
    val mmapedMaker = if (isWindows) rawMaker.fileMmapEnableIfSupported.fileMmapPreclearDisable.cleanerHackEnable else rawMaker
    mmapedMaker.make
  }

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  def makeOrGetDB[F[_]: Sync](path: Path): F[DB] = Sync[F].delay {
    cache.compute(path.toString, (k: String, db: DB) => Option(db) match {
      case Some(db) => db
      case None => mkDb(path)
    })}

  def shutdownAll[F[_]: Sync]: F[Unit] = Sync[F].delay { cache.synchronized {
    cache.forEach((k: String, db: DB) => {
      cache.remove(k)
      db.close
    })
  }}
}
