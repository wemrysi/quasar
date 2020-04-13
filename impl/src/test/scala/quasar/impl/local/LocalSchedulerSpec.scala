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

package quasar.impl.local

import slamdata.Predef._

import quasar.{EffectfulQSpec, ConditionMatchers}
import quasar.api.scheduler.SchedulerError._
import quasar.{concurrent => qc}
import quasar.impl.local.LocalScheduler._
import quasar.impl.storage.{IndexedStore, ConcurrentMapIndexedStore}

import cats.implicits._
import cats.effect._
import cats.effect.concurrent.Ref

import argonaut.{Argonaut, EncodeJson, DecodeJson}, Argonaut._

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import LocalSchedulerSpec._

class LocalSchedulerSpec(implicit ec: ExecutionContext) extends EffectfulQSpec[IO] with ConditionMatchers {
  lazy val blocker: Blocker = qc.Blocker.cached("local-scheduler-tests")

  val defaultTimer: Timer[IO] = IO.timer(ec)

  def mkTimer(timeRef: Ref[IO, Long], scale: Long): Timer[IO] = new Timer[IO] {
    def sleep(ft: FiniteDuration) = defaultTimer.sleep(ft / scale)
    def clock: Clock[IO] = new Clock[IO] {
      def realTime(unit: TimeUnit) = timeRef.get.map(unit.convert(_, MILLISECONDS))
      def monotonic(unit: TimeUnit) = timeRef.get.map(unit.convert(_, MILLISECONDS))
    }
  }

  def ephemeralStore[A]: IO[IndexedStore[IO, UUID, A]] =
    IO.delay(new ConcurrentHashMap[UUID, A]()) map (ConcurrentMapIndexedStore.unhooked[IO, UUID, A](_, blocker))

  def freshUUID: IO[UUID] = IO(UUID.randomUUID)

  "basic scheduling" >> {
    "once" >>* {
      for {
        ref <- Ref.of[IO, Long](0L)
        resultRef <- Ref.of[IO, List[String]](List[String]())
        implicit0(timer: Timer[IO]) = mkTimer(ref, 600L)
        implicit0(submit: Submit[IO, StringTrail]) = new Submit[IO, StringTrail] {
          def submit(a: StringTrail): IO[Unit] = resultRef.update { (lst: List[String]) =>
            a.value :: lst
          }
        }
        store <- ephemeralStore[Intention[StringTrail]]
        flags <- ephemeralStore[Long]

        trails <- LocalScheduler(freshUUID, store, flags, blocker) use { scheduler =>
          for {
            _ <- scheduler.addIntention(Intention(Period.Minutely, StringTrail("trail")).asJson)
            _ <- ref.set(60000L)
            _ <- timer.sleep(1.minute)
            result <- resultRef.get
          } yield result
        }
      } yield {
        trails must_=== List("trail")
      }
    }
    "long period" >>* {
      for {
        ref <- Ref.of[IO, Long](0L)
        resultRef <- Ref.of[IO, List[String]](List[String]())
        implicit0(timer: Timer[IO]) = mkTimer(ref, 600L)
        implicit0(submit: Submit[IO, StringTrail]) = new Submit[IO, StringTrail] {
          def submit(a: StringTrail): IO[Unit] = resultRef.update { (lst: List[String]) =>
            a.value :: lst
          }
        }
        store <- ephemeralStore[Intention[StringTrail]]
        flags <- ephemeralStore[Long]

        trails <- LocalScheduler(freshUUID, store, flags, blocker) use { scheduler =>
          for {
            _ <- scheduler.addIntention(Intention(Period.Minutely, StringTrail("trail")).asJson)
            _ <- (1 to 10).toList.traverse { i =>
              ref.set(i * 60000L) >> timer.sleep(1.minute)
            }
            result <- resultRef.get
          } yield result
        }
      } yield {
        trails must_=== List.fill(10)("trail")
      }
    }
    "multiple tasks" >>* {
      for {
        ref <- Ref.of[IO, Long](0L)
        resultRef <- Ref.of[IO, List[String]](List[String]())
        implicit0(timer: Timer[IO]) = mkTimer(ref, 600L)
        implicit0(submit: Submit[IO, StringTrail]) = new Submit[IO, StringTrail] {
          def submit(a: StringTrail): IO[Unit] = resultRef.update { (lst: List[String]) =>
            a.value :: lst
          }
        }
        store <- ephemeralStore[Intention[StringTrail]]
        flags <- ephemeralStore[Long]

        trails <- LocalScheduler(freshUUID, store, flags, blocker) use { scheduler =>
          for {
            _ <- scheduler.addIntention(Intention(Period.Minutely, StringTrail("0")).asJson)
            _ <- ref.set(60000L)
            _ <- timer.sleep(1.minute)
            _ <- scheduler.addIntention(Intention(Period.Minutely, StringTrail("1")).asJson)
            _ <- ref.set(2 * 60000L)
            _ <- timer.sleep(1.minute)
            result <- resultRef.get
          } yield result
        }
      } yield {
        trails.toSet must_=== Set("0", "1")
        trails.length must_=== 3
      }
    }
  }
  "basic CRUD" >>* {
    val initialIntention = Intention(Period.Minutely, StringTrail("0"))
    val editedIntention = Intention(Period.Hourly, StringTrail("1"))
    for {
      ref <- Ref.of[IO, Long](0L)
      resultRef <- Ref.of[IO, List[String]](List[String]())
      implicit0(timer: Timer[IO]) = mkTimer(ref, 600L)
      implicit0(submit: Submit[IO, StringTrail]) = new Submit[IO, StringTrail] {
        def submit(a: StringTrail): IO[Unit] = resultRef.update { (lst: List[String]) =>
          a.value :: lst
        }
      }
      store <- ephemeralStore[Intention[StringTrail]]
      flags <- ephemeralStore[Long]

      // It's a bit too complicated too test trails here, because `sleep` and `clock` aren't synchronized in `mkTimer` result
      (gotJson0, gotJson1, gotJson2, i) <- LocalScheduler(freshUUID, store, flags, blocker) use { scheduler =>
        for {
          ei <- scheduler.addIntention(initialIntention.asJson)
          i = ei.toOption.get
          gotJson0 <- scheduler.getIntention(i)
          _ <- scheduler.editIntention(i, editedIntention.asJson)
          gotJson1 <- scheduler.getIntention(i)
          _ <- scheduler.deleteIntention(i)
          gotJson2 <- scheduler.getIntention(i)
        } yield (gotJson0, gotJson1, gotJson2, i)
      }
    } yield {
      gotJson0.flatMap(_.as[Intention[StringTrail]].result) must beRight(initialIntention)
      gotJson1.flatMap(_.as[Intention[StringTrail]].result) must beRight(editedIntention)
      gotJson2 must beLeft(IntentionNotFound(i))
    }
  }
  "get intention stream" >>* {
    val intention0 = Intention(Period.Minutely, StringTrail("0"))
    val intention1 = Intention(Period.Minutely, StringTrail("1"))
    val intention2 = Intention(Period.Minutely, StringTrail("2"))
    val intention3 = Intention(Period.Minutely, StringTrail("3"))
    for {
      ref <- Ref.of[IO, Long](0L)
      resultRef <- Ref.of[IO, List[String]](List[String]())
      implicit0(timer: Timer[IO]) = mkTimer(ref, 600L)
      implicit0(submit: Submit[IO, StringTrail]) = new Submit[IO, StringTrail] {
        def submit(a: StringTrail): IO[Unit] = resultRef.update { (lst: List[String]) =>
          a.value :: lst
        }
      }
      store <- ephemeralStore[Intention[StringTrail]]
      flags <- ephemeralStore[Long]

      (set0, set1, set2, i0, i1, i2) <- LocalScheduler(freshUUID, store, flags, blocker) use { scheduler =>
        for {
          ei0 <- scheduler.addIntention(intention0.asJson)
          i0 = ei0.toOption.get
          ei1 <- scheduler.addIntention(intention1.asJson)
          i1 = ei1.toOption.get
          ei2 <- scheduler.addIntention(intention2.asJson)
          i2 = ei2.toOption.get

          set0 <- scheduler.intentions.compile.to(Set)

          _ <- scheduler.deleteIntention(i0)
          set1 <- scheduler.intentions.compile.to(Set)

          _ <- scheduler.editIntention(i2, intention3.asJson)
          set2 <- scheduler.intentions.compile.to(Set)

        } yield (set0, set1, set2, i0, i1, i2)
      }
    } yield {
      set0 must_=== Set((i0, intention0.asJson), (i1, intention1.asJson), (i2, intention2.asJson))
      set1 must_=== Set((i1, intention1.asJson), (i2, intention2.asJson))
      set2 must_=== Set((i1, intention1.asJson), (i2, intention3.asJson))
    }
  }

  "OnceAt" >> {
    "run once after its timestamp and saves a flag into flag store" >>* {
      val intention = Intention(Period.OnceAt(60000L), StringTrail("once"))
      for {
        ref <- Ref.of[IO, Long](0L)
        resultRef <- Ref.of[IO, List[String]](List[String]())
        implicit0(timer: Timer[IO]) = mkTimer(ref, 600L)
        implicit0(submit: Submit[IO, StringTrail]) = new Submit[IO, StringTrail] {
          def submit(a: StringTrail): IO[Unit] = resultRef.update { (lst: List[String]) =>
            a.value :: lst
          }
        }
        store <- ephemeralStore[Intention[StringTrail]]
        flags <- ephemeralStore[Long]

        (trails, i) <- LocalScheduler(freshUUID, store, flags, blocker) use { scheduler =>
          for {
            ei <- scheduler.addIntention(intention.asJson)
            i = ei.toOption.get
            _ <- ref.set(2 * 60L * 1000L)
            _ <- timer.sleep(10.minutes)
            trails <- resultRef.get
          } yield (trails, i)
        }
        mbFlag <- flags.lookup(i)
      } yield {
        mbFlag must beSome(2L * 60L * 1000L)
        trails must_=== List("once")
      }
    }
    "doesn't run if flag present, but run on next iteration if the flag is deleted" >>* {
      val intention = Intention(Period.OnceAt(60000L), StringTrail("once"))
      for {
        ref <- Ref.of[IO, Long](0L)
        resultRef <- Ref.of[IO, List[String]](List[String]())
        implicit0(timer: Timer[IO]) = mkTimer(ref, 600L)
        implicit0(submit: Submit[IO, StringTrail]) = new Submit[IO, StringTrail] {
          def submit(a: StringTrail): IO[Unit] = resultRef.update { (lst: List[String]) =>
            a.value :: lst
          }
        }
        store <- ephemeralStore[Intention[StringTrail]]
        flags <- ephemeralStore[Long]

        (trails0, trails1) <- LocalScheduler(freshUUID, store, flags, blocker) use { scheduler =>
          for {
            ei <- scheduler.addIntention(intention.asJson)
            i = ei.toOption.get
            _ <- flags.insert(i, 0L)
            _ <- ref.set(2 * 60L * 1000L)
            _ <- timer.sleep(10.minutes)
            trails0 <- resultRef.get
            _ <- flags.delete(i)
            _ <- timer.sleep(10.minutes)
            trails1 <- resultRef.get
          } yield (trails0, trails1)
        }
      } yield {
        trails0 must_=== List()
        trails1 must_=== List("once")
      }
    }
  }
  "correctly handle incorrect parameters" >>* {
    val intention = Intention(Period.Minutely, StringTrail("0"))
    for {
      ref <- Ref.of[IO, Long](0L)
      resultRef <- Ref.of[IO, List[String]](List[String]())
      implicit0(timer: Timer[IO]) = mkTimer(ref, 600L)
      implicit0(submit: Submit[IO, StringTrail]) = new Submit[IO, StringTrail] {
        def submit(a: StringTrail): IO[Unit] = resultRef.update { (lst: List[String]) =>
          a.value :: lst
        }
      }
      store <- ephemeralStore[Intention[StringTrail]]
      flags <- ephemeralStore[Long]

      (nonExistentId, e0, e1, e2, e3) <- LocalScheduler(freshUUID, store, flags, blocker) use { scheduler =>
        for {
          ei <- scheduler.addIntention(intention.asJson)
          i = ei.toOption.get

          e0 <- scheduler.addIntention(jNull)
          e1 <- scheduler.editIntention(i, jNull)
          nonExistentId <- freshUUID
          e2 <- scheduler.editIntention(nonExistentId, intention.asJson)
          e3 <- scheduler.deleteIntention(nonExistentId)
        } yield (nonExistentId, e0, e1, e2, e3)
      }
    } yield {
      e0 must beLeft(IncorrectIntention(jNull))
      e1 must beAbnormal(IncorrectIntention(jNull))
      e2 must beAbnormal(IntentionNotFound(nonExistentId))
      e3 must beAbnormal(IntentionNotFound(nonExistentId))
    }
  }
}

object LocalSchedulerSpec {
  final case class StringTrail(value: String)

  object StringTrail {
    implicit val encodeJson: EncodeJson[StringTrail] = EncodeJson { pl =>
      pl.value.asJson
    }
    implicit val decodeJson: DecodeJson[StringTrail] = DecodeJson { c =>
      c.as[String].map(StringTrail(_))
    }
  }
}
