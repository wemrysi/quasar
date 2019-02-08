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

import quasar.api.datasource.DatasourceType
import quasar.api.resource._
import quasar.connector._
import quasar.connector.ResourceError._
import quasar.connector.datasource.LightweightDatasource
import quasar.contrib.fs2.convert
import quasar.contrib.scalaz.MonadError_
import quasar.fp.ski.ι
import quasar.qscript.InterpretedRead

import scala.Predef.classOf

import java.nio.file.{Files, NoSuchFileException, Path => JPath}
import java.nio.file.attribute.BasicFileAttributes

import cats.effect.{ContextShift, Effect, Timer}
import fs2.Stream
import pathy.Path
import scalaz.{\/, OptionT, Scalaz}, Scalaz._
import shims._

/** A Datasource backed by the underlying filesystem local to Quasar.
  *
  * TODO: Currently only supports reading line-delimited JSON files (with
  *       support for the "precise" `Data` codec). Expand this via a pluggable
  *       set of decoders for CSV, XML, avro, etc.
  *
  * @param root the scope of this datasource, all paths will be considered relative to this one.
  * @param queryResult the evaluation strategy
  */
final class EvaluableLocalDatasource[F[_]: ContextShift: Timer] private (
    dsType: DatasourceType,
    root: JPath,
    queryResult: InterpretedRead[JPath] => QueryResult[F])(
    implicit F: Effect[F], RE: MonadResourceErr[F])
    extends LightweightDatasource[F, Stream[F, ?], QueryResult[F]] {

  val kind: DatasourceType = dsType

  def evaluate(ir: InterpretedRead[ResourcePath]): F[QueryResult[F]] =
    for {
      (jp, attrs) <- attributesOf(ir.path).getOrElseF(RE.raiseError(pathNotFound(ir.path)))
      _ <- attrs.isRegularFile.unlessM(RE.raiseError(notAResource(ir.path)))
    } yield queryResult(InterpretedRead(jp, ir.stages))

  def pathIsResource(path: ResourcePath): F[Boolean] =
    toNio[F](path) >>= (jp => F.delay(Files.isRegularFile(jp)))

  def prefixedChildPaths(path: ResourcePath): F[Option[Stream[F, (ResourceName, ResourcePathType)]]] = {
    def withType(jp: JPath): F[(ResourceName, ResourcePathType)] =
      F.delay(Files.isRegularFile(jp))
        .map(_.fold(ResourcePathType.leafResource, ResourcePathType.prefix))
        .strengthL(toResourceName(jp))

    val res = attributesOf(path) map {
      case (jp, attrs) if attrs.isDirectory =>
        convert.fromJavaStream(F.delay(Files.list(jp))).evalMap(withType)

      case _ => Stream.empty
    }

    res.run
  }

  ////

  private def attributesOf(resourcePath: ResourcePath): OptionT[F, (JPath, BasicFileAttributes)] =
    OptionT(toNio[F](resourcePath) flatMap { jpath =>
      F.recover(F.delay(some((jpath, Files.readAttributes(jpath, classOf[BasicFileAttributes]))))) {
        case _: NoSuchFileException => none
      }
    })

  private def toNio[F[_]: Effect](rp: ResourcePath): F[JPath] =
    Path.flatten("", "", "", ι, ι, rp.toPath).foldLeftM(root) { (p, n) =>
      if (n.isEmpty) p.point[F]
      else MonadError_[F, Throwable].unattempt_(\/.fromTryCatchNonFatal(p.resolve(n)))
    }

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  private def toResourceName(jp: JPath): ResourceName =
    ResourceName(jp.getFileName.toString)
}

object EvaluableLocalDatasource {
  def apply[F[_]: ContextShift: Effect: MonadResourceErr: Timer](
      dsType: DatasourceType,
      root: JPath)(
      queryResult: InterpretedRead[JPath] => QueryResult[F])
      : Datasource[F, Stream[F, ?], InterpretedRead[ResourcePath], QueryResult[F]] =
    new EvaluableLocalDatasource[F](dsType, root, queryResult)
}
