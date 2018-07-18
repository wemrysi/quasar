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

import quasar.contrib.pathy.{ADir, AFile}
import quasar.contrib.scalaz.MonadError_
import quasar.impl.storage.IndexedStore
import quasar.mimir.MimirCake.Cake
import quasar.precog.common.{Path => PrecogPath, RValue}
import quasar.yggdrasil.bytecode.JType
import quasar.yggdrasil.vfs.ResourceError

import cats.{Applicative, Monad, Show}
import cats.effect.LiftIO
import cats.instances.string._
import cats.syntax.applicative._
import cats.syntax.functor._
import cats.syntax.flatMap._
import cats.syntax.show._
import fs2.Stream
import monocle.Prism
import pathy.Path
import scalaz.{-\/, \/-}
import scalaz.std.option._
import shims._

final class MimirIndexedStore[F[_]: LiftIO: Monad] private (
    precog: Cake,
    tablesPrefix: ADir)(
    implicit ME: MonadError_[F, ResourceError])
    extends IndexedStore[F, String, RValue] {

  import precog.trans.constants._

  val entries: Stream[F, (String, RValue)] =
    for {
      rpaths <- Stream.eval(precog.vfs.ls(tablesPrefix).to[F])

      rpath <- Stream.emits(rpaths).covary[F]

      rfile <- Stream.emit(Path.refineType(rpath).toOption).unNone.covary[F]

      s = Path.fileName(rfile).value

      v <- Stream.eval(lookup(s)).unNone
    } yield (s, v)

  def lookup(s: String): F[Option[RValue]] = {
    def headValue(t: precog.Table): F[Option[RValue]] =
      for {
        firstSlice <-
          t.transform(SourceValue.Single)
            .slices
            .headOption
            .to[F]

        firstValue =
          firstSlice.filter(_.isDefinedAt(0)).map(_.toRValue(0))

      } yield firstValue

    val loaded =
      precog.Table.constString(Set(keyFileStr(s)))
        .load(JType.JUniverseT)
        .run.to[F]

    loaded flatMap {
      case \/-(t) => headValue(t)

      case -\/(ResourceError.NotFound(_)) => none[RValue].pure[F]

      case -\/(err) => ME.raiseError(err)
    }
  }

  def insert(s: String, v: RValue): F[Unit] =
    ME.unattempt(precog.ingest(
      PrecogPath(keyFileStr(s)),
      Stream.emit(v.toJValue)).run.to[F])

  def delete(s: String): F[Unit] =
    precog.vfs.delete(keyFile(s)).void.to[F]

  ////

  private def keyFile(s: String): AFile =
    tablesPrefix </> Path.file(s)

  private def keyFileStr(s: String): String =
    Path.posixCodec.printPath(keyFile(s))
}

object MimirIndexedStore {
  def apply[F[_]: LiftIO: Monad](
      precog: Cake,
      tablesPrefix: ADir)(
      implicit ME: MonadError_[F, ResourceError])
      : IndexedStore[F, String, RValue] =
    new MimirIndexedStore[F](precog, tablesPrefix)

  def transformIndex[F[_]: Monad, I, V](
      s: IndexedStore[F, String, V],
      indexName: String,
      prism: Prism[String, I])(
      implicit ME: MonadError_[F, ResourceError])
      : IndexedStore[F, I, V] =
    IndexedStore.xmapIndexF(s)(
      decodeP[F, String, I](indexName, prism))(
      i => prism(i).pure[F])

  def transformValue[F[_]: Monad, I, V](
      s: IndexedStore[F, I, RValue],
      valueName: String,
      prism: Prism[RValue, V])(
      implicit ME: MonadError_[F, ResourceError])
      : IndexedStore[F, I, V] =
    IndexedStore.xmapValueF(s)(
      decodeP[F, RValue, V](valueName, prism))(
      v => prism(v).pure[F])

  ////

  private def decodeP[F[_]: Applicative, A: Show, B](
      desc: String,
      prism: Prism[A, B])(
      implicit F: MonadError_[F, ResourceError])
      : A => F[B] =
    a => prism.getOption(a) match {
      case None =>
        F.raiseError(ResourceError.corrupt(
          s"Failed to decode '$desc' from '${a.show}'"))

      case Some(b) =>
        b.pure[F]
    }
}
