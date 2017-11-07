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

package quasar.fp

import slamdata.Predef._

import scalaz.Forall

/**
 * Represents a rank-1 existential skolem, which is to say it also
 * represents a rank-2 universal.  This is just a different encoding
 * of scalaz.Forall.  This encoding is useful because it type infers
 * much more nicely in many cases.  The cast in the implementation is
 * provably sound, we just can't prove it to the compiler.
 */
sealed trait τ

object τ {

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def apply[F[_]](ft: F[τ]): Forall[F] =
    new Forall[F] { def apply[A] = ft.asInstanceOf[F[A]] }
}
