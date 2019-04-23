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

package quasar.impl.external

import slamdata.Predef._
import quasar.concurrent.BlockingContext
import quasar.connector.{DestinationModule, HeavyweightDatasourceModule, LightweightDatasourceModule}
import quasar.contrib.fs2.convert
import quasar.fp.ski.κ
import quasar.impl.DatasourceModule

import java.lang.{
  Class,
  ClassCastException,
  ClassLoader,
  ClassNotFoundException,
  ExceptionInInitializerError,
  IllegalAccessException,
  IllegalArgumentException,
  NoSuchFieldException,
  NullPointerException,
  Object
}
import java.nio.file.{Files, Path}
import java.util.jar.JarFile

import argonaut.Json
import cats.effect.{ConcurrentEffect, ContextShift, Effect, Sync, Timer}
import cats.syntax.applicativeError._
import fs2.{Chunk, Stream}
import fs2.io.file
import jawnfs2._
import org.slf4s.Logging
import org.typelevel.jawn.AsyncParser
import org.typelevel.jawn.support.argonaut.Parser._
import scalaz.syntax.tag._

object ExternalDatasources extends Logging {
  import ExternalConfig._

  val PluginChunkSize = 8192

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  def apply[F[_]: ContextShift: Timer](
      config: ExternalConfig,
      blockingPool: BlockingContext)(
      implicit F: ConcurrentEffect[F])
      : Stream[F, (List[DatasourceModule], List[DestinationModule])] = {

    val datasourceModuleStream: Stream[F, DatasourceModule] = config match {
      case PluginDirectory(directory) =>
        Stream.eval(F.delay((Files.exists(directory), Files.isDirectory(directory)))) flatMap {
          case (true, true) =>
            convert.fromJavaStream(F.delay(Files.list(directory)))
              .filter(_.getFileName.toString.endsWith(PluginExtSuffix))
              .flatMap(loadPlugin[F](_, blockingPool))
              .flatMap((loadDatasourceModule[F](_, _)).tupled)

          case (true, false) =>
            warnStream[F](s"Unable to load plugins from '$directory', does not appear to be a directory", None)

          case _ =>
            Stream.empty
        }

      case PluginFiles(files) =>
        Stream.emits(files)
          .flatMap(loadPlugin[F](_, blockingPool))
          .flatMap((loadDatasourceModule[F](_, _)).tupled)

      case ExplodedDirs(modules) =>
        for {
          exploded <- Stream.emits(modules)

          (cn, cp) = exploded

          classLoader <- Stream.eval(ClassPath.classLoader[F](ParentCL, cp))
          mod <- loadDatasourceModule[F](cn.value, classLoader)
        } yield mod
    }

    val destinationModuleStream: Stream[F, DestinationModule] = config match {
      case PluginDirectory(directory) =>
        Stream.eval(F.delay((Files.exists(directory), Files.isDirectory(directory)))) flatMap {
          case (true, true) =>
            convert.fromJavaStream(F.delay(Files.list(directory)))
              .filter(_.getFileName.toString.endsWith(PluginExtSuffix))
              .flatMap(loadPlugin[F](_, blockingPool))
              .flatMap((loadDestinationModule[F](_, _)).tupled)

          case (true, false) =>
            warnStream[F](s"Unable to load plugins from '$directory', does not appear to be a directory", None)

          case _ =>
            Stream.empty
        }

      case PluginFiles(files) =>
        Stream.emits(files)
          .flatMap(loadPlugin[F](_, blockingPool))
          .flatMap((loadDestinationModule[F](_, _)).tupled)

      case ExplodedDirs(modules) =>
        for {
          exploded <- Stream.emits(modules)

          (cn, cp) = exploded

          classLoader <- Stream.eval(ClassPath.classLoader[F](ParentCL, cp))
          mod <- loadDestinationModule[F](cn.value, classLoader)
        } yield mod
    }

    for {
      ds <- datasourceModuleStream.fold(List.empty[DatasourceModule])((m, d) => d :: m)
      _ <- infoStream[F](s"Loaded ${ds.length} datasource(s)")
      dts <- destinationModuleStream.fold(List.empty[DestinationModule])((m, d) => d :: m)
      _ <- infoStream[F](s"Loaded ${dts.length} destination(s)")
    } yield (ds, dts)
  }

  ////

  private val ParentCL = this.getClass.getClassLoader

  private val PluginExtSuffix = "." + Plugin.FileExtension

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def loadModule[A, F[_]: Sync](clazz: Class[_])(f: Object => A): Stream[F, A] =
    Stream.eval(Sync[F].delay(f(clazz.getDeclaredField("MODULE$").get(null))))

  def loadClass[F[_]: Sync](cn: String, classLoader: ClassLoader): Stream[F, Class[_]] =
    Stream.eval(Sync[F].delay(classLoader.loadClass(cn))) recoverWith {
      case cnf: ClassNotFoundException =>
        warnStream[F](s"Could not locate class for datasource module '$cn'", Some(cnf))
    }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def loadDestinationModule[F[_]: Sync](
    className: String, classLoader: ClassLoader)
      : Stream[F, DestinationModule] = {
    def handleFailedDestination[A](s: Stream[F, A]): Stream[F, A] =
      s recoverWith {
        case e @ (_: NoSuchFieldException | _: IllegalAccessException | _: IllegalArgumentException | _: NullPointerException) =>
          warnStream[F](s"Destination module '$className' does not appear to be a singleton object", Some(e))

        case e: ExceptionInInitializerError =>
          warnStream[F](s"Destination module '$className' failed to load with exception", Some(e))

        case _: ClassCastException =>
          infoStream[F](s"Module '$className' does not support writeback") >> Stream.empty
      }

    def loadDestination(clazz: Class[_]): Stream[F, DestinationModule] =
      loadModule(clazz)(_.asInstanceOf[DestinationModule])

    for {
      clazz <- loadClass(className, classLoader)
      destination <- handleFailedDestination(loadDestination(clazz))
    } yield destination
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def loadDatasourceModule[F[_]: Sync](
    className: String, classLoader: ClassLoader): Stream[F, DatasourceModule] = {
    def handleFailedDatasource[A](s: Stream[F, A]): Stream[F, A] =
      s recoverWith {
        case e @ (_: NoSuchFieldException | _: IllegalAccessException | _: IllegalArgumentException | _: NullPointerException) =>
          warnStream[F](s"Datasource module '$className' does not appear to be a singleton object", Some(e))

        case e: ExceptionInInitializerError =>
          warnStream[F](s"Datasource module '$className' failed to load with exception", Some(e))

        case _: ClassCastException =>
          warnStream[F](s"Datasource module '$className' is not actually a subtype of LightweightDatasourceModule or HeavyweightDatasourceModule", None)
      }

    def loadLightweight(clazz: Class[_]): Stream[F, DatasourceModule] =
      loadModule(clazz) { o =>
        DatasourceModule.Lightweight(o.asInstanceOf[LightweightDatasourceModule])
      }

    def loadHeavyweight(clazz: Class[_]): Stream[F, DatasourceModule] =
      loadModule(clazz) { o =>
        DatasourceModule.Heavyweight(o.asInstanceOf[HeavyweightDatasourceModule])
      }

    for {
      clazz <- loadClass(className, classLoader)
      datasource <- handleFailedDatasource(loadLightweight(clazz) handleErrorWith κ(loadHeavyweight(clazz)))
    } yield datasource
  }

  private def loadPlugin[F[_]: ContextShift: Effect: Timer](
    pluginFile: Path,
    blockingPool: BlockingContext)
      : Stream[F, (String, ClassLoader)] =
    for {
      js <-
        file.readAll[F](pluginFile, blockingPool.unwrap, PluginChunkSize)
          .chunks
          .map(_.toByteBuffer)
          .parseJson[Json](AsyncParser.SingleValue)

      pluginResult <- Stream.eval(Plugin.fromJson[F](js))

      plugin <- pluginResult.fold(
        (s, c) => warnStream[F](s"Failed to decode plugin from '$pluginFile': $s ($c)", None),
        r => Stream.eval(r.withAbsolutePaths[F](pluginFile.getParent)))

      classLoader <- Stream.eval(ClassPath.classLoader[F](ParentCL, plugin.classPath))

      mainJar = new JarFile(plugin.mainJar.toFile)

      backendModuleAttr <- jarAttribute[F](mainJar, Plugin.ManifestAttributeName)
      versionModuleAttr <- jarAttribute[F](mainJar, Plugin.ManifestVersionName)

      _ <- versionModuleAttr match {
        case None => warnStream[F](s"No '${Plugin.ManifestVersionName}' attribute found in Manifest for '$pluginFile'.", None)
        case Some(version) => infoStream[F](s"Loading $pluginFile with version $version")
      }

      moduleClasses <- backendModuleAttr match {
        case None =>
          warnStream[F](s"No '${Plugin.ManifestAttributeName}' attribute found in Manifest for '$pluginFile'.", None)

        case Some(attr) =>
          Stream.emit(attr.split(" "))
      }

      moduleClass <- if (moduleClasses.isEmpty)
        warnStream[F](s"No classes defined for '${Plugin.ManifestAttributeName}' attribute in Manifest from '$pluginFile'.", None)
      else
        Stream.chunk(Chunk.array(moduleClasses)).covary[F]
    } yield (moduleClass, classLoader)

  private def jarAttribute[F[_]: Sync](j: JarFile, attr: String): Stream[F, Option[String]] =
    Stream.eval(Sync[F].delay(Option(j.getManifest.getMainAttributes.getValue(attr))))

  private def warnStream[F[_]: Sync](msg: => String, cause: Option[Throwable]): Stream[F, Nothing] =
    Stream.eval(Sync[F].delay(cause.fold(log.warn(msg))(log.warn(msg, _)))).drain

  private def infoStream[F[_]: Sync](msg: => String): Stream[F, Unit] =
    Stream.eval(Sync[F].delay(log.info(msg)))
}
