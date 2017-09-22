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

package quasar.contrib.specs2

import slamdata.Predef._

import org.specs2.execute._

/**
  * Specs2 trait that is like "pendingUntilFixed" but additionally tracks
  * actual results. It fails a test in case the expected actual is not equal to
  * the (actual) actual.
  */
trait PendingWithActualTracking {

  implicit def toPendingWithActualTracking[T : AsResult](t: =>T)
    : PendingWithActualTracking[T] = new PendingWithActualTracking(t)

  private def c(msg: String, extra: String) = if (msg == "") msg else msg + extra

  class PendingWithActualTracking[T : AsResult](t: =>T) {
    def pendingWithActual(m: String, expectedActual: String): Result = ResultExecution.execute(AsResult(t)) match {
      case s @ Success(_,_) =>
        Failure(m + " Fixed now, you should remove the 'pendingWithActual' marker")
      case f @ Failure(msg, e, stackTrace, FailureDetails(actual, expected)) =>
        if (actual != expectedActual)
          Failure(m + " Behaviour has changed. Please review the test and set new expectation. New actual is: " + actual)
        else
          Pending(m + " Pending until fixed (actual unchanged)")
      case other =>
        Failure(m + " Behaviour has changed. Please review the test and set new expectation. Test result is: " + other)
    }
  }
}

object PendingWithActualTracking extends PendingWithActualTracking
