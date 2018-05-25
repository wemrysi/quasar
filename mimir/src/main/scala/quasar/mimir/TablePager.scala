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

package quasar.mimir

import quasar.Data
import quasar.blueeyes.json.JValue
import quasar.contrib.fs2._
import quasar.contrib.scalaz.concurrent._
import quasar.yggdrasil.table.{ColumnarTableModule, Slice}

import delorean._

import cats.effect.IO

import fs2.async
import fs2.async.mutable.Queue
import fs2.interop.scalaz._

import scalaz.{\/, -\/, \/-, ~>, Functor, StreamT}
import scalaz.concurrent.Task
import scalaz.syntax.monad._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

import java.util.concurrent.atomic.AtomicBoolean

trait TablePagerModule extends ColumnarTableModule {

  final class TablePager private (
      slices: StreamT[Task, Slice],
      queue: Queue[Task, Throwable \/ Vector[Data]]) {

    private val running = new AtomicBoolean(true)

    {
      val driver = slices foreachRec { slice =>
        for {
          flag <- Task.delay(running.get())

          _ <- if (flag && !slice.isEmpty) {
            val json = slice.toJsonElements.map(JValue.toData)

            if (json.isEmpty)
              Task.now(())
            else
              queue.enqueue1(\/-(json))
          } else {
            // we can't terminate early, because there are no finalizers in StreamT
            Task.now(())
          }
        } yield ()
      }

      // enqueue the empty vector so ReadFile.scan knows when to stop scanning
      val ta = driver >> queue.enqueue1(\/-(Vector.empty))

      ta unsafePerformAsync {
        case -\/(t) => queue.enqueue1(-\/(t)).unsafePerformAsync(_ => ())
        case \/-(_) => ()
      }
    }

    def more: Task[Vector[Data]] =
      queue.dequeue1.flatMap(_.fold(Task.fail, Task.now))

    def close: Task[Unit] = Task.delay(running.set(false))
  }

  object TablePager {
    def apply(table: Table, lookahead: Int = 1): IO[TablePager] = {
      for {
        q <- async.boundedQueue[Task, Throwable \/ Vector[Data]](lookahead).to[IO]
        // ambiguity between M and effect-derived monad
        slices = table.slices.trans(λ[IO ~> Task](_.to[Task]))
        back <- IO(new TablePager(slices, q))
      } yield back
    }
  }
}
