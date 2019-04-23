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
import quasar.connector.DestinationModule

import java.lang.{
  Class,
  ClassCastException,
  ClassLoader,
  ExceptionInInitializerError,
  IllegalAccessException,
  IllegalArgumentException,
  NoSuchFieldException,
  NullPointerException
}

import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import cats.syntax.applicativeError._
import fs2.Stream
import org.slf4s.Logging

object ExternalDestinations extends Logging {
  def apply[F[_]: ContextShift: Timer](
      config: ExternalConfig,
      blockingPool: BlockingContext)(
      implicit F: ConcurrentEffect[F])
      : Stream[F, List[DestinationModule]] = {
    val destinationModuleStream: Stream[F, DestinationModule] =
      ExternalModules(config, blockingPool).flatMap((loadDestinationModule[F](_, _)).tupled)

    for {
      dts <- destinationModuleStream.fold(List.empty[DestinationModule])((m, d) => d :: m)
      _ <- infoStream[F](s"Loaded ${dts.length} destination(s)")
    } yield dts
  }

  ////

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
      ExternalModules.loadModule(clazz)(_.asInstanceOf[DestinationModule])

    for {
      clazz <- ExternalModules.loadClass(className, classLoader)
      destination <- handleFailedDestination(loadDestination(clazz))
    } yield destination
  }

  private def warnStream[F[_]: Sync](msg: => String, cause: Option[Throwable]): Stream[F, Nothing] =
    Stream.eval(Sync[F].delay(cause.fold(log.warn(msg))(log.warn(msg, _)))).drain

  private def infoStream[F[_]: Sync](msg: => String): Stream[F, Unit] =
    Stream.eval(Sync[F].delay(log.info(msg)))
}
