/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.impl.storage

import slamdata.Predef._

import quasar.concurrent.BlockingContext
import quasar.impl.cluster.Timestamped

import cats.effect.{IO, Resource, Timer, ContextShift, Concurrent}
import cats.effect.concurrent.Ref

import fs2.Stream

import scalaz.std.string._

import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Random

import shims._

final class TimestampedStoreSpec extends IndexedStoreSpec[IO, String, String] {
  implicit val ec: ExecutionContext = ExecutionContext.global
  implicit val timer: Timer[IO] = IO.timer(ec)

  type Persistence = ConcurrentHashMap[String, Timestamped[String]]
  type UnderlyingStore = IndexedStore[IO, String, Timestamped[String]]

  val pool = BlockingContext.cached("timestamped-spec-pool")

  val underlying: Resource[IO, IndexedStore[IO, String, Timestamped[String]]] =
    Resource.liftF[IO, Persistence](IO(new ConcurrentHashMap[String, Timestamped[String]]()))
      .map(ConcurrentMapIndexedStore.unhooked[IO, String, Timestamped[String]](_, pool))

  val emptyStore: Resource[IO, IndexedStore[IO, String, String]] = for {
    u <- underlying
    res <- TimestampedStore(u, pool)
  } yield res

  val valueA = "A"
  val valueB = "B"
  val freshIndex = IO(Random.nextInt().toString)

  "timestamp store" >> {
    "lookup for timestamped" >>* {
      underlying.use { (us: UnderlyingStore) => for {
        bar <- Timestamped.tagged[IO, String]("bar")
        _ <- us.insert("foo", bar)
        ts <- TimestampedStore(us, pool).use(IO(_))
        res <- ts.lookup("foo")
      } yield {
        res mustEqual Some("bar")
      }}
    }
    "inserted values are timestamps" >>* {
      underlying.use { (us: UnderlyingStore) => for {
        ts <- TimestampedStore(us, pool).use(IO(_))
        _ <- ts.insert("foo", "bar")
        bar <- us.lookup("foo")
      } yield {
        bar.flatMap(Timestamped.raw(_)) mustEqual Some("bar")
      }}
    }
    "deletion preserves tombstones" >>* {
      underlying.use { (us: UnderlyingStore) => for {
        start <- timer.clock.realTime(MILLISECONDS)
        ts <- TimestampedStore(us, pool).use(IO(_))
        _ <- ts.insert("foo", "bar")
        _ <- ts.delete("foo")
        t <- us.lookup("foo")
        stop <- timer.clock.realTime(MILLISECONDS)
      } yield {
        t must beLike {
          case Some(Timestamped.Tombstone(stamp)) =>
            stamp must be_>=(start)
            stamp must be_<=(stop)
        }
      }}
    }
    "timestamps works" >>* {
      underlying.use { (us: UnderlyingStore) => for {
        ts <- TimestampedStore(us, pool).use(IO(_))
        _ <- ts.insert("foo", "bar")
        _ <- ts.delete("foo")
        _ <- ts.insert("foo", "baz")
        _ <- ts.insert("bar", "foo")
        foo <- us.lookup("foo")
        bar <- us.lookup("bar")
        timestamps <- ts.timestamps
      } yield {
        timestamps.size mustEqual 2
        timestamps.get("foo") mustEqual(foo.map(Timestamped.timestamp(_)))
        timestamps.get("bar") mustEqual(bar.map(Timestamped.timestamp(_)))
      }}
    }
  }
}
