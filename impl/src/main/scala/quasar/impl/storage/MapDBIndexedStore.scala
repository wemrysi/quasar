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

import cats.effect.{LiftIO, Sync, IO}
import cats.effect.concurrent.Ref
import cats.instances.list._
import cats.syntax.foldable._
import cats.syntax.applicative._
import cats.syntax.functor._
import cats.syntax.flatMap._
import cats.syntax.apply._
import fs2.Stream
import shims._

import java.nio.file.Path

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

  def unsafeClose: F[Unit] = Sync[F].delay(db.close)

  private def commit: F[Unit] = Sync[F].delay(db.commit)

}
object MapDBIndexedStore {
  val TREEMAP_NAME: String = "main"

  val ref: Ref[IO, Map[String, DB]] = Ref.unsafe(Map.empty)

  def apply[F[_]: Sync: LiftIO](path: Path): F[IndexedStore[F, String, String]] =
    makeOrGetDB[F](path) flatMap { db => Sync[F].delay(new MapDBIndexedStore[F](db)) }

  def mkDb[F[_]: Sync](path: Path): F[DB] =
    Sync[F].delay(DBMaker.fileDB(path.toFile).checksumHeaderBypass.closeOnJvmShutdown.fileLockDisable.make)

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  def makeOrGetDB[F[_]: Sync: LiftIO](path: Path): F[DB] =
    LiftIO[F].liftIO(ref.get) flatMap { (cache: Map[String, DB]) => cache.get(path.toString) match {
      case Some(a) => a.pure[F]
      case None => for {
        db <- mkDb(path)
        _ <- LiftIO[F].liftIO(ref.set(cache + ((path.toString, db))))
      } yield db
    }}

  def shutdownAll[F[_]: Sync: LiftIO]: F[Unit] =
    LiftIO[F].liftIO(ref.get) flatMap { (cache: Map[String, DB]) => cache.toList.traverse_ { case (_, db) =>
      Sync[F].delay(db.close)
    }} productR { LiftIO[F].liftIO(ref.set(Map.empty)) }
}
