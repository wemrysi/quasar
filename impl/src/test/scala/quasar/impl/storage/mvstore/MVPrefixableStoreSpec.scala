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

import slamdata.Predef.{Eq => _, _}

import quasar.impl.storage
import quasar.impl.storage._

import scala.concurrent.ExecutionContext.Implicits.global

import cats._
import cats.effect.{Blocker, IO, Resource}
import cats.implicits._

import org.h2.mvstore._

import scala.util.Random
import java.util.UUID

import MVPrefixableStoreSpec._

final class MVPrefixableStoreSpec extends IndexedStoreSpec[IO, Array[String], Int] {
  val emptyStore: Resource[IO, PrefixableStore[IO, Array[String], Int]] = {
    storage.offheapMVStore[IO].evalMap { db =>
      val store: MVMap[Array[String], Int] = db.openMap("test")
      MVPrefixableStore[IO, String, Int](store, Blocker.liftExecutionContext(global))
    }
  }
  val freshIndex: IO[Array[String]] = for {
    len <- IO(Random.nextInt(10))
    // 1/100 chance to fail w/o `+ 1`
    emptyList = List.fill(len + 1)(0)
    list <- emptyList.traverse { _ => IO(UUID.randomUUID.toString) }
  } yield Array(list:_*)
  val valueA = 12
  val valueB = 14

  "prefixable store" >> {
    "prefixed entries" >> {
      "equal to lookup when prefix is the same as key" >>* {
        emptyStore use { store =>
          for {
            k1 <- freshIndex
            k2 <- freshIndex
            k3 <- freshIndex

            _ <- store.insert(k1, 1)
            _ <- store.insert(k2, 2)
            _ <- store.insert(k3, 3)

            vs <- store.prefixedEntries(k2).compile.toList

            v <- store.lookup(k2)
          } yield {
            vs.map(_._2) must_=== v.toList
          }
        }
      }
      "topmost prefix" >>* {
        emptyStore use { store =>
          val prefix = "prefix"
          val k1 = Array(prefix, "1")
          val k2 = Array(prefix, "2", "12")
          val k3 = Array(prefix, "3", "43", "abcdefg")
          for {
            _ <- store.insert(k1, 1)
            _ <- store.insert(k2, 2)
            _ <- store.insert(k3, 3)

            vs <- store.prefixedEntries(Array(prefix)).compile.toList
          } yield {
            vs.map(_._2) must contain(exactly(1, 2, 3))
          }
        }
      }

      "mid prefix" >>* {
        emptyStore use { store =>
          val prefix = Array("foo", "bar")
          val k1 = Array("foo", "bar", "baz")
          val k2 = Array("foo", "bar", "quux")
          val k3 = Array("bar", "baz", "a", "b")
          for {
            _ <- store.insert(k1, 1)
            _ <- store.insert(k2, 2)
            _ <- store.insert(k3, 3)

            vs <- store.prefixedEntries(prefix).compile.toList
          } yield {
            vs.map(_._2) must contain(exactly(1, 2))
          }
        }
      }

      "entire prefix must match" >>* {
        emptyStore use { store =>
          val prefix = Array("a", "b")
          val k1 = prefix ++: Array("c")
          val k2 = prefix ++: Array("d", "e")
          val k3 = prefix ++: Array("f", "g")
          for {
            _ <- store.insert(k1, 1)
            _ <- store.insert(k2, 2)
            _ <- store.insert(k3, 3)

            vs <- store.prefixedEntries(Array("b", "d")).compile.toList
          } yield {
            vs must beEmpty
          }
        }
      }

      "empty array returns all entries" >>* {
        emptyStore use { store =>
          val k1 = Array("a")
          val k2 = Array("b", "c")
          val k3 = Array("a", "d", "e")

          for {
            _ <- store.insert(k1, 1)
            _ <- store.insert(k2, 2)
            _ <- store.insert(k3, 3)

            all <- store.entries.compile.toList
            prefixed <- store.prefixedEntries(Array()).compile.toList
          } yield {
            prefixed must containTheSameElementsAs(all)
          }
        }
      }
    }

    "delete prefixed" >> {
      "only removes prefixed" >>* {
        emptyStore use { store =>
          val k1 = Array("prefix", "a")
          val k2 = Array("prefix", "b")
          val k3 = Array("foo")

          for {
            _ <- store.insert(k1, 1)
            _ <- store.insert(k2, 2)
            _ <- store.insert(k3, 3)

            _ <- store.deletePrefixed(Array("prefix"))
            vs <- store.entries.compile.toList
          } yield {
            vs must contain(exactly(k3 -> 3))
          }
        }
      }

      "noop if no prefixed" >>* {
        emptyStore use { store =>
          val k1 = Array("prefix", "foo")
          val k2 = Array("prefix", "bar", "baz")
          val k3 = Array("prefix")

          for {
            _ <- store.insert(k1, 1)
            _ <- store.insert(k2, 2)
            _ <- store.insert(k3, 3)

            preDelete <- store.entries.compile.toList

            _ <- store.deletePrefixed(Array("bar"))

            postDelete <- store.entries.compile.toList

          } yield {
            postDelete must containTheSameElementsAs(preDelete)
          }
        }
      }

      "remove everything when prefix is empty" >>* {
        emptyStore use { store =>
          for {
            _ <- store.insert(Array("a"), 1)
            _ <- store.insert(Array("b", "c"), 2)
            _ <- store.insert(Array(), 0)
            _ <- store.insert(Array("de", "a", "f"), 3)
            _ <- store.deletePrefixed(Array())

            postDelete <- store.entries.compile.toList
          } yield {
            postDelete must beEmpty
          }
        }
      }
    }
  }

}

object MVPrefixableStoreSpec {
  implicit val arrayEq: Eq[Array[String]] = Eq.by { (x: Array[String]) =>
    List[String](x:_*)
  }

  implicit val arrayShow: Show[Array[String]] = Show.show { (x: Array[String]) =>
    x.mkString("Array(", ", ", ")")
  }
}
