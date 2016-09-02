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

import scalaz._

/** Half of `Inject`, but composable. An instance is available whenever the
  * corresponding `Inject` is. */
final case class Inj[F[_], G[_]](run: F ~> G) {
  def apply[A](fa: F[A]): G[A] = run(fa)

  def compose[H[_]](that: Inj[H, F]): Inj[H, G] =
    Inj(run compose that.run)
}
object Inj {
  def apply[F[_], G[_]](implicit inj: Inj[F, G]): Inj[F, G] = inj

  // NB: Somewhat awkward here, but this syntax already existed in this package
  def unapply[F[_], G[_], A](ga: G[A])(implicit prj: Prj[F, G]): Option[F[A]] =
    prj(ga)

  implicit def fromInject[F[_], G[_]](implicit I: Inject[F, G]): Inj[F, G] = Inj(I)
}
