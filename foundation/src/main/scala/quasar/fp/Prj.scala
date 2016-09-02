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

package quasar.fp

import quasar.Predef.Option

import scalaz.{Inject, ~>}

/** The other half of `Inject`, but composable.  An instance is available
  * whenever the corresponding `Inject` is. */
final case class Prj[F[_], G[_]](run: G ~> λ[α => Option[F[α]]]) {
  def apply[A](ga: G[A]): Option[F[A]] = run(ga)

  def compose[H[_]](that: Prj[H, F]): Prj[H, G] =
    Prj(
      new (G ~> λ[α => Option[H[α]]]) {
        def apply[A](ga: G[A]): Option[H[A]] =
          run(ga).flatMap(that.run(_))
      })
}
object Prj {
  def apply[F[_], G[_]](implicit prj: Prj[F, G]): Prj[F, G] = prj

  implicit def fromInject[F[_], G[_]](implicit I: Inject[F, G]): Prj[F, G] =
    Prj(
      new (G ~> λ[α => Option[F[α]]]) {
        def apply[A](ga: G[A]): Option[F[A]] =
          I.prj(ga)
      })
}
