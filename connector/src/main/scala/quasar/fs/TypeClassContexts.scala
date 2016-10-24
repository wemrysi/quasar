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

package quasar.fs

import quasar.Predef._
import scalaz._

/** These traits are intended for the common situation where
 *  a lot of code is being written inside a particular context.
 *  For instance, all the [Query|Read|Write|Manage]File
 *  implementations will require an Applicative instance, and
 *  filesystem results are of type EitherT[F, FilesystemError, ?].
 *
 *  What these allow is the extrusion of all the boilerplate
 *  necessary to lift values between levels if those levels are
 *  treated entirely generically. The Either implicits are the
 *  clearest example. One may variously need to lift a value of
 *  type A into F[A], F[_ \/ A], or EitherT[F, _, A]. Done
 *  manually, each of these liftings requires its own distinct
 *  tedious and noisy syntax.
 *
 *  But types exist to relieve us of such drudgery.
 */
trait ApplicativeContext[F[_]] {
  protected implicit def applicative: Applicative[F]
  protected implicit def point[A](x: A): F[A] = applicative point x
}
trait MonadicContext[F[_]] extends ApplicativeContext[F] {
  protected implicit def applicative: Monad[F]
}
trait EitherTypes[F[X], L] {
  type LR[A]  = L \/ A
  type ER[A]  = EitherT[F, L, A]
  type FLR[A] = F[LR[A]]
}
trait EitherTContextLeft[F[X], L] extends ApplicativeContext[F] with EitherTypes[F, L] {
  implicit protected def leftT[A](x: L): ER[A]   = EitherT left x
  implicit protected def rightT[A](x: A): ER[A]  = EitherT right x
  implicit protected def leftL[A](x: L): LR[A]   = -\/(x)
  implicit protected def rightL[A](x: A): LR[A]  = \/-(x)
  implicit protected def leftP[A](x: L): FLR[A]  = point(-\/(x))
  implicit protected def rightP[A](x: A): FLR[A] = point(\/-(x))
}
trait StateContext[S] {
  type F[A]  = State[S, A]
  type FR[A] = S -> A
}
