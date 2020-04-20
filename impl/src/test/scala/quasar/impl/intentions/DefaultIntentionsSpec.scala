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

package quasar.impl.intentions

import slamdata.Predef._

import quasar.{EffectfulQSpec, Condition, ConditionMatchers}
import quasar.api.intentions.IntentionError, IntentionError._
import quasar.connector.scheduler.Scheduler

import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._

import fs2.Stream

import scala.concurrent.ExecutionContext

final class DefaultIntentionsSpec(implicit ec: ExecutionContext) extends EffectfulQSpec[IO] with ConditionMatchers {
  "intentions" >> {
    def scheduler(mp: Ref[IO, Map[Int, String]]): Scheduler[IO, Int, String] = new Scheduler[IO, Int, String] {
      def entries = {
        Stream.eval(mp.get) flatMap (x => Stream.emits(x.toList))
      }
      def addIntention(c: String) = for {
        container <- mp.get
        ix = container.size
        _ <- mp.update(_.updated(ix, c))
      } yield Right(ix)
      def lookupIntention(i: Int) =
        mp.get map (_.get(i).toRight(IntentionNotFound(i)))
      def editIntention(i: Int, c: String) = for {
        container <- mp.get
        result <- container.get(i) match {
          case None => Condition.abnormal(IntentionNotFound(i)).pure[IO]
          case Some(_) => mp.update(_.updated(i, c)) as Condition.normal[IntentionNotFound[Int]]()
        }
      } yield result
      def deleteIntention(i: Int) = for {
        container <- mp.get
        result <- container.get(i) match {
          case None => Condition.abnormal(IntentionNotFound(i)).pure[IO]
          case Some(_) => mp.update(_ - i) as Condition.normal[IntentionNotFound[Int]]()
        }
      } yield result
    }
    def mkIntentions(is: Set[Int], s: Scheduler[IO, Int, String]) = DefaultIntentions[IO, Int, Int, String](
      Stream.emits(is.toList),
      x => if (is.contains(x)) s.pure[Option].pure[IO] else Option.empty[Scheduler[IO, Int, String]].pure[IO])

    "allIntentions multiplies provided scheduler ids with intention ids" >>* {
      for {
        ref <- Ref.of[IO, Map[Int, String]](Map(0 -> "foo"))
        intentions0 = mkIntentions(Set(0), scheduler(ref))
        intentions1 = mkIntentions(Set(1, 2), scheduler(ref))
        intentions2 = mkIntentions(Set(), scheduler(ref))
        all0 <- intentions0.allIntentions.compile.toList
        all1 <- intentions1.allIntentions.compile.toList
        all2 <- intentions2.allIntentions.compile.toList
      } yield {
        all0 must_=== List((0, 0, "foo"))
        all1 must_=== List((1, 0, "foo"), (2, 0, "foo"))
        all2 must_=== List()
      }
    }
    "schedulerIntentions is proxied from scheduler" >>* {
      for {
        ref <- Ref.of[IO, Map[Int, String]](Map(0 -> "foo", 1 -> "bar"))
        intentions = mkIntentions(Set(0), scheduler(ref))
        eis <- intentions.schedulerIntentions(0)
        is = eis.toOption.get
        result <- is.compile.toList
      } yield {
        result must_=== List((0, "foo"), (1, "bar"))
      }
    }
    "add proxies to scheduler" >>* {
      for {
        ref <- Ref.of[IO, Map[Int, String]](Map())
        intentions = mkIntentions(Set(0), scheduler(ref))
        _ <- intentions.add(0, "foo")
        result <- ref.get
      } yield {
        result must_=== Map(0 -> "foo")
      }
    }
    "lookup proxies to scheduler" >>* {
      for {
        ref <- Ref.of[IO, Map[Int, String]](Map())
        intentions = mkIntentions(Set(0), scheduler(ref))
        res0 <- intentions.lookup(0, 0)
        _ <- ref.update(_.updated(0, "foo"))
        res1 <- intentions.lookup(0, 0)
      } yield {
        res0 must beLeft(IntentionNotFound(0))
        res1 must beRight("foo")
      }
    }
    "delete proxies to scheduler" >>* {
      for {
        ref <- Ref.of[IO, Map[Int, String]](Map(0 -> "foo"))
        intentions = mkIntentions(Set(0), scheduler(ref))
        _ <- intentions.delete(0, 0)
        result <- ref.get
      } yield {
        result must_=== Map()
      }
    }
    "methods (except allIntentions) should return an error when scheduler not found" >>* {
      for {
        ref <- Ref.of[IO, Map[Int, String]](Map(0 -> "foo"))
        intentions = mkIntentions(Set(), scheduler(ref))
        is <- intentions.schedulerIntentions(0)
        added <- intentions.add(0, "foo")
        looked <- intentions.lookup(0, 0)
        edited <- intentions.edit(0, 0, "foo")
        deleted <- intentions.delete(0, 0)
      } yield {
        is must beLeft(SchedulerNotFound(0))
        added must beLeft(SchedulerNotFound(0))
        looked must beLeft(SchedulerNotFound(0))
        edited must beAbnormal(SchedulerNotFound(0))
        deleted must beAbnormal(SchedulerNotFound(0))
      }
    }
  }
}
