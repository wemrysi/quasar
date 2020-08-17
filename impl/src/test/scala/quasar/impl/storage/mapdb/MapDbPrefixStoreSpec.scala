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

package quasar.impl.storage.mapdb

import slamdata.Predef.{Eq => _, _}

import quasar.impl.storage._

import java.util.UUID
import java.{lang => jl}

import scala.Predef.classOf
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

import cats._
import cats.derived.auto.eq._
import cats.effect.{Blocker, IO, Resource}
import cats.implicits._

import org.mapdb.{DBMaker, Serializer}

import shapeless._

import MapDbPrefixStoreSpec._

final class MapDbPrefixStoreSpec extends IndexedStoreSpec[IO, UUID :: jl.String :: jl.Integer :: HNil, jl.Long] {
  implicit class ExtraIntOps(val i: Int) {
    def jint: jl.Integer = jl.Integer.valueOf(i)
    def jlong: jl.Long = jl.Long.valueOf(i.toLong)
  }

  val emptyStore: Resource[IO, PrefixStore[IO, UUID :: jl.String :: jl.Integer :: HNil, jl.Long]] =
    Resource.make(IO(DBMaker.memoryDB().make()))(db => IO(db.close())) flatMap { db =>
      MapDbPrefixStore[IO](
        "mapdb-prefix-store-spec",
        db,
        Serializer.UUID :: Serializer.STRING :: Serializer.INTEGER :: HNil,
        Serializer.LONG,
        Blocker.liftExecutionContext(global))
    }

  val freshIndex: IO[UUID :: jl.String :: jl.Integer :: HNil] =
    for {
      uuid <- IO(UUID.randomUUID)
      str <- IO(Random.alphanumeric.take(6).mkString)
      int <- IO(Random.nextInt(100000).jint)
    } yield uuid :: str :: int :: HNil

  val valueA = 24.jlong
  val valueB = 42.jlong

  "prefix store" >> {
    "prefixed entries" >> {
      "equal to lookup when prefix is same as key" >>* {
        emptyStore use { store =>
          for {
            k1 <- freshIndex
            k2 <- freshIndex
            k3 <- freshIndex

            _ <- store.insert(k1, 1.jlong)
            _ <- store.insert(k2, 2.jlong)
            _ <- store.insert(k3, 3.jlong)

            vs <- store.prefixedEntries(k2).compile.toList

            v <- store.lookup(k2)
          } yield {
            vs.map(_._2) must_=== v.toList
          }
        }
      }

      "topmost prefix" >>* {
        emptyStore use { store =>
          for {
            uuid <- IO(UUID.randomUUID)

            k1 = uuid :: "foo" :: 1.jint :: HNil
            k2 = uuid :: "foo" :: 2.jint :: HNil
            k3 = uuid :: "bar" :: 3.jint :: HNil

            _ <- store.insert(k1, 1.jlong)
            _ <- store.insert(k2, 2.jlong)
            _ <- store.insert(k3, 3.jlong)

            vs <- store.prefixedEntries(uuid :: HNil).compile.toList
          } yield {
            vs.map(_._2) must contain(exactly(1.jlong, 2.jlong, 3.jlong))
          }
        }
      }

      "mid prefix" >>* {
        emptyStore use { store =>
          for {
            uuid <- IO(UUID.randomUUID)

            k1 = uuid :: "foo" :: 1.jint :: HNil
            k2 = uuid :: "foo" :: 2.jint :: HNil
            k3 = uuid :: "bar" :: 3.jint :: HNil

            _ <- store.insert(k1, 1.jlong)
            _ <- store.insert(k2, 2.jlong)
            _ <- store.insert(k3, 3.jlong)

            vs <- store.prefixedEntries(uuid :: "foo" :: HNil).compile.toList
          } yield {
            vs.map(_._2) must contain(exactly(1.jlong, 2.jlong))
          }
        }
      }

      "entire prefix must match" >>* {
        emptyStore use { store =>
          for {
            uuid <- IO(UUID.randomUUID)
            otherUuid <- IO(UUID.randomUUID)

            k1 = uuid :: "foo" :: 1.jint :: HNil
            k2 = uuid :: "foo" :: 2.jint :: HNil
            k3 = uuid :: "bar" :: 3.jint :: HNil

            _ <- store.insert(k1, 1.jlong)
            _ <- store.insert(k2, 2.jlong)
            _ <- store.insert(k3, 3.jlong)

            vs <- store.prefixedEntries(otherUuid :: "foo" :: HNil).compile.toList
          } yield {
            vs must beEmpty
          }
        }
      }
    }

    "delete prefixed" >> {
      "only removes prefixed" >>* {
        emptyStore use { store =>
          for {
            uuid <- IO(UUID.randomUUID)

            k1 = uuid :: "foo" :: 1.jint :: HNil
            k2 = uuid :: "foo" :: 2.jint :: HNil
            k3 = uuid :: "bar" :: 3.jint :: HNil

            _ <- store.insert(k1, 1.jlong)
            _ <- store.insert(k2, 2.jlong)
            _ <- store.insert(k3, 3.jlong)

            _ <- store.deletePrefixed(uuid :: "foo" :: HNil)
            vs <- store.entries.compile.toList
          } yield {
            vs must contain(exactly(k3 -> 3.jlong))
          }
        }
      }

      "noop if no prefixed" >>* {
        emptyStore use { store =>
          for {
            uuid <- IO(UUID.randomUUID)
            otherUuid <- IO(UUID.randomUUID)

            k1 = uuid :: "foo" :: 1.jint :: HNil
            k2 = uuid :: "foo" :: 2.jint :: HNil
            k3 = uuid :: "bar" :: 3.jint :: HNil

            _ <- store.insert(k1, 1.jlong)
            _ <- store.insert(k2, 2.jlong)
            _ <- store.insert(k3, 3.jlong)

            preDelete <- store.entries.compile.toList
            _ <- store.deletePrefixed(otherUuid :: "foo" :: HNil)
            postDelete <- store.entries.compile.toList
          } yield {
            postDelete must containTheSameElementsAs(preDelete)
          }
        }
      }
    }
  }
}

object MapDbPrefixStoreSpec {
  implicit val jIntegerEq: Eq[jl.Integer] =
    Eq.by(_.intValue)

  implicit def testHListSHow[L <: HList]: Show[L] =
    Show.fromToString

  implicit val jlLongEq: Eq[jl.Long] =
    Eq.by(_.longValue)

  implicit val jlLongShow: Show[jl.Long] =
    Show.fromToString
}
