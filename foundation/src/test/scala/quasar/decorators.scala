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

package quasar

import quasar.Predef._
import org.specs2.execute._
import quasar.build.BuildInfo._

/**
 Allows the body of an example to be marked as pending when the specification
 is run normally, but skipped when a special flag is enabled, so that the code
 it purports to test will not be erroneously flagged as covered.
 */
trait PendingWithAccurateCoverage extends PendingUntilFixed {
  outer =>

  implicit class QuasarOpsForAsResultable[T: AsResult](t: => T) {
    /** Steps in front of the standard specs2 implicit. */
    def pendingUntilFixed: Result = pendingUntilFixed("")
    def pendingUntilFixed(m: String): Result = (
      if (coverageEnabled) Skipped(m + " (pending example skipped during coverage run)")
      else if (isCIBuild) Skipped(m + " (pending example skipped during CI build)")
      else outer.toPendingUntilFixed(t).pendingUntilFixed(m)
    )

    def skippedOnUserEnv: Result            = skippedOnUserEnv("")
    def skippedOnUserEnv(m: String): Result = if (isIsolatedEnv) AsResult(t) else Skipped(m)
  }
}
