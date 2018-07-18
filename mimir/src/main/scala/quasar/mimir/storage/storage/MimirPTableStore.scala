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

package quasar.mimir.storage

import slamdata.Predef._

import quasar.contrib.fs2.convert._
import quasar.contrib.pathy.{ADir, AFile}
import quasar.mimir.MimirCake.Cake
import quasar.yggdrasil.ExactSize
import quasar.niflheim.NIHDB
import quasar.yggdrasil.nihdb.NIHDBProjection

import cats.arrow.FunctionK
import cats.effect.{IO, LiftIO}

import fs2.Stream

import pathy.Path

import scalaz.{Monad, OptionT}
import scalaz.syntax.monad._

import shims._

/*
 * We don't abstract this because read (and write) use the path-dependent type based on the
 * value of `cake`. It would be nearly impossible to get the types to work out correctly in
 * an abstraction, and probably just yield all sorts of problems for users of this functionality.
 */
final class MimirPTableStore[F[_]: Monad: LiftIO] private (
    val cake: Cake,
    tablesPrefix: ADir) {

  import cake.{Table => PTable}

  // has overwrite semantics
  def write(key: StoreKey, table: PTable): Stream[F, Unit] = {
    val ios = Stream.bracket(cake.createDB(keyToFile(key)).map(_.toOption))({
      case Some((blob, version, db)) =>
        fromStreamT(table.slices).zipWithIndex evalMap {
          case (slice, offset) =>
            val jvs = slice.toJsonElements
            IO.fromFuture(IO(db.insertVerified(NIHDB.Batch(offset, jvs.toList) :: Nil)))
        }

      case None =>
        Stream.empty
    }, {
      case Some((blob, version, db)) =>
        IO.fromFuture(IO(db.cook)) *> cake.commitDB(blob, version, db)

      case None =>
        IO.pure(())
    })

    ios.translate(λ[FunctionK[IO, F]](LiftIO[F].liftIO(_))).drain
  }

  def read(key: StoreKey): F[Option[PTable]] = {
    val ioaOpt: OptionT[IO, PTable] = for {
      // this just swallows error messages; should probably log them or something
      db <- OptionT(cake.openDB(keyToFile(key)).run.map(_.flatMap(_.toOption)))
      proj <- NIHDBProjection.wrap(db).liftM[OptionT]
    } yield PTable(proj.getBlockStream(None), ExactSize(proj.length))

    LiftIO[F].liftIO(ioaOpt.run)
  }

  def exists(key: StoreKey): F[Boolean] =
    read(key).map(_.isDefined)

  def delete(key: StoreKey): F[Unit] = {
    val path = keyToFile(key)
    LiftIO[F].liftIO(cake.closeDB(path) *> cake.fs.delete(path).as(()))
  }

  private[this] def keyToFile(key: StoreKey): AFile =
    tablesPrefix </> Path.file(key.value)
}

object MimirPTableStore {

  def apply[F[_]: Monad: LiftIO](cake: Cake, tablesPrefix: ADir): MimirPTableStore[F] =
    new MimirPTableStore[F](cake, tablesPrefix)
}
