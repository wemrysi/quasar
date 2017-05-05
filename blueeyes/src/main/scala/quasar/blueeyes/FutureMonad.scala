/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.blueeyes

import scala.concurrent.{Await, Future, Promise}
import java.util.concurrent.ExecutorService

import scalaz._

class FutureMonad(context: ExecutorService) extends Applicative[Future] with Monad[Future] {
  def point[A](a: => A) = Future(a)(context)
  def bind[A, B](fut: Future[A])(f: A => Future[B]) = fut.flatMap(f)
  override def ap[A,B](fa: => Future[A])(ff: => Future[A => B]) =
    for {
      a <- fa
      f <- ff
    } yield f(a)
}

object FutureMonad {
  def M(implicit context: ExecutorService): Monad[Future] = new FutureMonad(context)
}
