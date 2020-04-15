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

import quasar.api.datasource.DatasourceType
import quasar.api.destination.DestinationType
import quasar.api.scheduler.SchedulerType

import cats.ApplicativeError

import IncompatibleModuleException._

sealed trait IncompatibleModuleException extends java.lang.RuntimeException with Product with Serializable { self =>
  override def getMessage = self match {
    case Datasource(k) => s"Loaded datasource implementation with type $k is incompatible with quasar"
    case Destination(k) => s"Loaded destination implementation with type $k is incompatible with quasar"
    case Scheduler(k) => s"Loaded scheduler implementation with type $k is incompatible with quasar"
  }
}

object IncompatibleModuleException {
  final case class Datasource(kind: DatasourceType) extends IncompatibleModuleException
  final case class Destination(kind: DestinationType) extends IncompatibleModuleException
  final case class Scheduler(kind: SchedulerType) extends IncompatibleModuleException

  def linkDatasource[F[_]: ApplicativeError[?[_], Throwable], A](kind: DatasourceType, fa: => F[A]): F[A] =
    link(Datasource(kind), fa)

  def linkDestination[F[_]: ApplicativeError[?[_], Throwable], A](kind: DestinationType, fa: => F[A]): F[A] =
    link(Destination(kind), fa)

  def linkScheduler[F[_]: ApplicativeError[?[_], Throwable], A](kind: SchedulerType, fa: => F[A]): F[A] =
    link(Scheduler(kind), fa)

  private def link[F[_]: ApplicativeError[?[_], Throwable], A](ex: IncompatibleModuleException, fa: => F[A]): F[A] =
    try {
      fa
    } catch {
      case linkError: java.lang.LinkageError =>
        ApplicativeError[F, Throwable].raiseError(ex.initCause(linkError))
    }

}
