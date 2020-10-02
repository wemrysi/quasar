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

package quasar.impl.storage.mvstore

import slamdata.Predef.{Eq => _, uuid => _, _}

import quasar.contrib.scalaz.MonadError_
import quasar.impl.storage
import quasar.impl.storage._

import java.util.UUID

import scala.concurrent.ExecutionContext.Implicits.global

import cats._
import cats.derived.auto.eq._
import cats.effect.{Blocker, IO, Resource}
import cats.implicits._

import scala.util.Random
import java.util.UUID

import scodec._

import shapeless._

import MVPrefixStoreSpec._

final class MVPrefixStoreSpec extends IndexedStoreSpec[IO, UUID :: String :: Int :: HNil, Long] {
  val emptyStore: Resource[IO, PrefixStore.SCodec[IO, UUID :: String :: Int :: HNil, Long]] =
    storage.offheapMVStore[IO] evalMap { db =>
      MVPrefixStore[IO, UUID :: String :: Int :: HNil, Long](db, "testing", Blocker.liftExecutionContext(global))
    }

  val freshIndex: IO[UUID :: String :: Int :: HNil] =
    for {
      uuid <- IO(UUID.randomUUID)
      str <- IO(Random.alphanumeric.take(6).mkString)
      int <- IO(Random.nextInt(10000))
    } yield uuid :: str :: int :: HNil

  val valueA = 24L
  val valueB = 42L

  "prefix store" >> {
    "prefixed entries" >> {
      "equal to lookup when prefix is same as key" >>* {
        emptyStore use { store =>
          for {
            k1 <- freshIndex
            k2 <- freshIndex
            k3 <- freshIndex

            _ <- store.insert(k1, 1L)
            _ <- store.insert(k2, 2L)
            _ <- store.insert(k3, 3L)

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

            k1 = uuid :: "foo" :: 1 :: HNil
            k2 = uuid :: "foo" :: 2 :: HNil
            k3 = uuid :: "bar" :: 3 :: HNil

            _ <- store.insert(k1, 1L)
            _ <- store.insert(k2, 2L)
            _ <- store.insert(k3, 3L)

            vs <- store.prefixedEntries(uuid :: HNil).compile.toList
          } yield {
            vs.map(_._2) must contain(exactly(1L, 2L, 3L))
          }
        }
      }

      "mid prefix" >>* {
        emptyStore use { store =>
          for {
            uuid <- IO(UUID.randomUUID)

            k1 = uuid :: "foo" :: 1 :: HNil
            k2 = uuid :: "foo" :: 2 :: HNil
            k3 = uuid :: "bar" :: 3 :: HNil

            _ <- store.insert(k1, 1L)
            _ <- store.insert(k2, 2L)
            _ <- store.insert(k3, 3L)

            vs <- store.prefixedEntries(uuid :: "foo" :: HNil).compile.toList
          } yield {
            vs.map(_._2) must contain(exactly(1L, 2L))
          }
        }
      }

      "entire prefix must match" >>* {
        emptyStore use { store =>
          for {
            uuid <- IO(UUID.randomUUID)
            otherUuid <- IO(UUID.randomUUID)

            k1 = uuid :: "foo" :: 1 :: HNil
            k2 = uuid :: "foo" :: 2 :: HNil
            k3 = uuid :: "bar" :: 3 :: HNil

            _ <- store.insert(k1, 1L)
            _ <- store.insert(k2, 2L)
            _ <- store.insert(k3, 3L)

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

            k1 = uuid :: "foo" :: 1 :: HNil
            k2 = uuid :: "foo" :: 2 :: HNil
            k3 = uuid :: "bar" :: 3 :: HNil

            _ <- store.insert(k1, 1L)
            _ <- store.insert(k2, 2L)
            _ <- store.insert(k3, 3L)

            _ <- store.deletePrefixed(uuid :: "foo" :: HNil)
            vs <- store.entries.compile.toList
          } yield {
            vs must contain(exactly(k3 -> 3L))
          }
        }
      }

      "noop if no prefixed" >>* {
        emptyStore use { store =>
          for {
            uuid <- IO(UUID.randomUUID)
            otherUuid <- IO(UUID.randomUUID)

            k1 = uuid :: "foo" :: 1 :: HNil
            k2 = uuid :: "foo" :: 2 :: HNil
            k3 = uuid :: "bar" :: 3 :: HNil

            _ <- store.insert(k1, 1L)
            _ <- store.insert(k2, 2L)
            _ <- store.insert(k3, 3L)

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

object MVPrefixStoreSpec {
  import scodec.codecs._

  implicit def testHListShow[L <: HList]: Show[L] = Show.fromToString

  implicit val ioStoreError: MonadError_[IO, StoreError] =
    MonadError_.facet[IO](StoreError.throwableP)

  implicit val uuidCodec: Codec[UUID] = uuid

  implicit val strCodec: Codec[String] = utf8_32

  implicit val intCodec: Codec[Int] = int32

  implicit val longCodec: Codec[Long] = int64
}
