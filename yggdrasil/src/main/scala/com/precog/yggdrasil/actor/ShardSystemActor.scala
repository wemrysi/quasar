/*
 *  ____    ____    _____    ____    ___     ____
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either version
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog.yggdrasil
package actor

import blueeyes._
import com.precog.common._
import com.precog.common.security.PermissionsFinder
import com.precog.common.ingest._

import akka.util.duration._
import akka.pattern.gracefulStop

import blueeyes.json._
import blueeyes.bkka.Stoppable

import org.slf4s._
import org.streum.configrity.converter.Extra._

import scalaz.{ Failure, Success }
import scalaz.syntax.applicative._
import scalaz.syntax.semigroup._
import scalaz.syntax.std.boolean._
import scalaz.std.option._
import scalaz.std.map._
import scalaz.std.vector._

// type ShardSystemActor

trait ShardConfig extends BaseConfig {
  type IngestConfig

  def shardId: String
  def logPrefix: String

  def ingestConfig: Option[IngestConfig]

  def stopTimeout: Timeout = config[Long]("actors.stop.timeout", 300) seconds

  def batchStoreDelay: Duration = config[Long]("actors.store.idle_millis", 1000) millis
}

// The ingest system consists of the ingest supervisor and ingest actor(s)
case class IngestSystem(ingestActor: ActorRef, stoppable: Stoppable)

object IngestSystem extends Logging {
  def actorStop(config: ShardConfig, actor: ActorRef, name: String)(implicit system: ActorSystem, executor: ExecutionContext): Future[Unit] = {
    for {
      _ <- Future(log.debug(config.logPrefix + " Stopping " + name + " actor within " + config.stopTimeout.duration))
      b <- gracefulStop(actor, config.stopTimeout.duration)
    } yield {
      log.debug(config.logPrefix + " Stop call for " + name + " actor returned " + b)
    }
  } recover {
    case e => log.error("Error stopping " + name + " actor", e)
  }
}
