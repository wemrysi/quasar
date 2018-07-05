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

package quasar.impl.datasource.local

import slamdata.Predef.{Stream => _, Seq => _, _}
import quasar.Data
import quasar.DataCodec.Precise
import quasar.api.{DataSourceType, ResourceName, ResourcePath, ResourcePathType}
import quasar.api.ResourceError._
import quasar.connector.DataSource
import quasar.connector.datasource.LightweightDataSource
import quasar.contrib.fs2.convert
import quasar.contrib.scalaz.MonadError_
import quasar.fp.ski.ι

import java.nio.file.{Files, Path => JPath}
import java.text.ParseException

import scala.collection.JavaConverters._

import argonaut.Json
import cats.effect.{Effect, Sync, Timer}
import fs2.{io, Chunk, Stream}
import jawn.support.argonaut.Parser.facade
import jawnfs2._
import pathy.Path
import scalaz.{\/, EitherT, Scalaz}, Scalaz._
import shims._

/** A DataSource backed by the underlying filesystem local to Quasar.
  *
  * TODO: Currently only supports reading line-delimited JSON files (with
  *       support for the "precise" `Data` codec). Expand this via a pluggable
  *       set of decoders for CSV, XML, avro, etc.
  *
  * @param root the scope of this datasource, all paths will be considered relative to this one.
  * @param readChunkSizeBytes the number of bytes per chunk to use when reading files.
  */
final class LocalDataSource[F[_]: Sync, G[_]: Effect: Timer] private (
    root: JPath,
    readChunkSizeBytes: Int)
    extends LightweightDataSource[F, Stream[G, ?], Stream[G, Data]] {

  val kind: DataSourceType = LocalType

  val shutdown: F[Unit] = ().point[F]

  def evaluate(path: ResourcePath): F[ReadError \/ Stream[G, Data]] = {
    val dataStream =
      for {
        jp <- toNio(path).liftM[EitherT[?[_], ReadError, ?]]

        _ <- EitherT(F.delay(Files.exists(jp)).map { exists =>
          if (exists) ().right[ReadError]
          else pathNotFound[ReadError](path).left[Unit]
        })

        _ <- EitherT(F.delay(Files.isRegularFile(jp)).map { isFile =>
          if (isFile) ().right[ReadError]
          else notAResource[ReadError](path).left[Unit]
        })
      } yield {
        io.file.readAllAsync[G](jp, readChunkSizeBytes)
          .chunks
          .map(_.toByteBuffer)
          .parseJsonStream[Json]
          .chunks
          .flatMap(decodeChunk)
      }

    dataStream.run
  }

  def children(path: ResourcePath): F[CommonError \/ Stream[G, (ResourceName, ResourcePathType)]] = {
    def withType(jp: JPath): G[(ResourceName, ResourcePathType)] =
      G.delay(Files.isRegularFile(jp))
        .map(_.fold(ResourcePathType.resource, ResourcePathType.resourcePrefix))
        .strengthL(toResourceName(jp))

    ifExists[CommonError](path)(jp =>
      convert.fromJavaStream(G.delay(Files.list(jp))).evalMap(withType))
  }

  def descendants(path: ResourcePath): F[CommonError \/ Stream[G, ResourcePath]] = {
    def emitFile(p: JPath): Stream[G, JPath] =
      Stream.eval(G.delay(Files.isRegularFile(p)))
        .ifM(Stream.emit(p), Stream.empty)

    ifExists[CommonError](path)(jp =>
      convert.fromJavaStream(G.delay(Files.walk(jp)))
        .flatMap(emitFile)
        .map(f => fromNio(root.relativize(f))))
  }

  def isResource(path: ResourcePath): F[Boolean] =
    toNio(path) >>= (jp => F.delay(Files.isRegularFile(jp)))

  ////

  private val F = Sync[F]
  private val G = Effect[G]
  private val ME = MonadError_[F, Throwable]

  private def decodeChunk(c: Chunk[Json]): Stream[G, Data] =
    c.traverse(Precise.decode)
      .leftMap(e => new ParseException(e.message, -1))
      .fold(Stream.raiseError, Stream.chunk)
      .covary[G]

  private object ifExists {
    def apply[E >: CommonError] = new PartiallyApplied[E]
    final class PartiallyApplied[E >: CommonError] {
      def apply[A](rp: ResourcePath)(f: JPath => A): F[E \/ A] =
        for {
          jp <- toNio(rp)

          exists <- F.delay(Files.exists(jp))

          ea = if (exists)
            f(jp).right[E]
          else
            pathNotFound[E](rp).left[A]
        } yield ea
    }
  }

  private def fromNio(jp: JPath): ResourcePath =
    jp.iterator.asScala
      .map(toResourceName)
      .foldLeft(ResourcePath.root())(_ / _)

  private def toNio(rp: ResourcePath): F[JPath] =
    Path.flatten("", "", "", ι, ι, rp.toPath).foldLeftM(root) { (p, n) =>
      if (n.isEmpty) p.point[F]
      else ME.unattempt_(\/.fromTryCatchNonFatal(p.resolve(n)))
    }

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  private def toResourceName(jp: JPath): ResourceName =
    ResourceName(jp.getFileName.toString)
}

object LocalDataSource {
  def apply[F[_]: Sync, G[_]: Effect: Timer](
      root: JPath,
      readChunkSizeBytes: Int)
      : DataSource[F, Stream[G, ?], ResourcePath, Stream[G, Data]] =
    new LocalDataSource[F, G](root, readChunkSizeBytes)
}
