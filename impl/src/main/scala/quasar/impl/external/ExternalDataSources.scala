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

import slamdata.Predef.{Array, Option, String, SuppressWarnings}
import quasar.api.DataSourceType
import quasar.connector.{HeavyweightDataSourceModule, LightweightDataSourceModule}
import quasar.contrib.fs2.convert
import quasar.contrib.fs2.stream._
import quasar.impl.DataSourceModule

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
import cats.effect.{Effect, Sync, Timer}
import cats.syntax.applicativeError._
import fs2.{Segment, Stream}
import fs2.io.file
import jawn.AsyncParser
import jawn.support.argonaut.Parser._
import jawnfs2._
import scalaz.IMap

object ExternalDataSources {
  import ExternalConfig._

  val PluginChunkSize = 8192

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  def apply[F[_]: Timer](config: ExternalConfig)(implicit F: Effect[F])
      : Stream[F, IMap[DataSourceType, DataSourceModule]] = {

    val moduleStream = config match {
      case PluginDirectory(directory) =>
        convert.fromJavaStream(F.delay(Files.list(directory)))
          .filter(_.getFileName.toString.endsWith(PluginExtSuffix))
          .flatMap(loadPlugin[F](_))

      case ExplodedDirs(modules) =>
        for {
          exploded <- Stream.emits(modules)

          (cn, cp) = exploded

          classLoader <- ClassPath.classLoader[Stream[F, ?]](ParentCL, cp)

          module <- loadModule[F](cn.value, classLoader)
        } yield module
    }

    moduleStream.fold(IMap.empty[DataSourceType, DataSourceModule])(
      (m, d) => m.insert(d.kind, d))
  }

  ////

  private val ParentCL = this.getClass.getClassLoader

  private val PluginExtSuffix = "." + Plugin.FileExtension

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def loadModule[F[_]: Sync](className: String, classLoader: ClassLoader)
      : Stream[F, DataSourceModule] = {

    def loadModule0(clazz: Class[_])(f: Object => DataSourceModule): Stream[F, DataSourceModule] =
      Sync[Stream[F, ?]]
        .delay(f(clazz.getDeclaredField("MODULE$").get(null)))
        .recoverWith {
          case e @ (_: NoSuchFieldException | _: IllegalAccessException | _: IllegalArgumentException | _: NullPointerException) =>
            // TODO{logging}
            //log.warn(s"Datasource module '$className' does not appear to be a singleton object", e)
            Stream.empty

          case e: ExceptionInInitializerError =>
            // TODO{logging}
            //log.warn(s"Datasource module '$className' failed to load with exception", e)
            Stream.empty

          case _: ClassCastException =>
            // TODO{logging}
            //log.warn(s"Datasource module '$className' is not actually a subtype of LightweightDataSourceModule or HeavyweightDataSourceModule")
            Stream.empty
        }

    def loadLightweight(clazz: Class[_]): Stream[F, DataSourceModule] =
      loadModule0(clazz) { o =>
        DataSourceModule.Lightweight(o.asInstanceOf[LightweightDataSourceModule])
      }

    def loadHeavyweight(clazz: Class[_]): Stream[F, DataSourceModule] =
      loadModule0(clazz) { o =>
        DataSourceModule.Heavyweight(o.asInstanceOf[HeavyweightDataSourceModule])
      }

    for {
      clazz <- Sync[Stream[F, ?]].delay(classLoader.loadClass(className)) recoverWith {
        case cnf: ClassNotFoundException =>
          // TODO{logging}
          //log.warn(s"Could not locate class for Datasource module '$className'", cnf)
          Stream.empty
      }

      module <- loadLightweight(clazz) ++ loadHeavyweight(clazz)
    } yield module
  }

  private def loadPlugin[F[_]: Effect: Timer](pluginFile: Path): Stream[F, DataSourceModule] = {
    val S = Sync[Stream[F, ?]]

    val module: Stream[F, DataSourceModule] = for {
      js <-
        file.readAllAsync[F](pluginFile, PluginChunkSize)
          .chunks
          .map(_.toByteBuffer)
          .parseJson[Json](AsyncParser.SingleValue)

      plugin <-
        Plugin.fromJson[Stream[F, ?]](js)
          .map(_.toOption)
          .unNone
          .flatMap(_.withAbsolutePaths[Stream[F, ?]](pluginFile.getParent))

      classLoader <- ClassPath.classLoader[Stream[F, ?]](ParentCL, plugin.classPath)

      mainJar = new JarFile(plugin.mainJar.toFile)

      backendModuleAttr <-
        S.delay(Option(
          mainJar
            .getManifest
            .getMainAttributes
            .getValue(Plugin.ManifestAttributeName)))
          .unNone

      moduleClass <- Stream.segment(Segment.array(backendModuleAttr.split(" ")))

      mod <- loadModule[F](moduleClass, classLoader)
    } yield mod

    /* TODO{logging}
    for {
      result <- backend.run
      _ <- if (result.isEmpty) {
        Task.delay(log.warn(s"unable to load any backends from $pluginFile; perhaps the file is invalid or the 'Backend-Module' attribute is not defined"))
      } else Task.now(())
    } yield result
    */

    module
  }
}
