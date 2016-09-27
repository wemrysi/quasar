/*
 * Copyright 2014–2016 SlamData Inc.
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

package ygg

import quasar.Predef._
import scala.util._
import scala.AnyVal

package object macros {
  type Vec[+A] = scala.Vector[A]
  val Vec      = scala.Vector

  def doTry[A](body: => A): Try[A] = Try(body)

  implicit class TryOps[A](private val x: Try[A]) extends AnyVal {
    def |(expr: => A): A = fold(_ => expr, x => x)
    def fold[B](f: Throwable => B, g: A => B): B = x match {
      case Success(x) => g(x)
      case Failure(t) => f(t)
    }
  }
}
