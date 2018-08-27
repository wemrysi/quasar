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

package quasar.yggdrasil.nihdb

import quasar.niflheim.{Chef, V1CookedBlockFormat, V1SegmentFormat, VersionedCookedBlockFormat, VersionedSegmentFormat}

import akka.actor.{ActorRef, ActorSystem, Props}

import org.specs2.specification.AfterAll

import scala.concurrent.duration._

import java.util.concurrent.{ScheduledThreadPoolExecutor, ThreadFactory}
import java.util.concurrent.atomic.AtomicInteger

trait NIHDBAkkaSetup extends AfterAll {

  // cargo-culted all this setup, mostly from Precog
  val CookThreshold = 1
  val Timeout = 300.seconds

  val TxLogScheduler = new ScheduledThreadPoolExecutor(20,
    new ThreadFactory {
      private val counter = new AtomicInteger(0)

      def newThread(r: Runnable): Thread = {
        val t = new Thread(r)
        t.setName("HOWL-sched-%03d".format(counter.getAndIncrement()))

        t
      }
    })

  implicit val actorSystem = ActorSystem(
    "nihdbExecutorActorSystem",
    classLoader = Some(getClass.getClassLoader))

  private val props: Props = Props(Chef(
    VersionedCookedBlockFormat(Map(1 -> V1CookedBlockFormat)),
    VersionedSegmentFormat(Map(1 -> V1SegmentFormat))))

  val masterChef: ActorRef = actorSystem.actorOf(props)

  override def afterAll(): Unit = {
    actorSystem.terminate
  }
}
