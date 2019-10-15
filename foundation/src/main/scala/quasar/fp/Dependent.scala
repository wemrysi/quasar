/*
 * Copyright 2014–2019 SlamData Inc.
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

import cats.~>

/**
 * This a bit weird, but it's useful when you have an F which forms
 * a GADT over some closed algebra of A, and where some class C may
 * be materialized for every member of that set, but only if you know
 * *which* A you have. This is distinct from `FunctionK` because it
 * is intended to be implicit.
 */
trait Dependent[F[_], +C[_]] {
  def apply[A](fa: F[A]): C[A]
  def lift[C2[a] >: C[a]]: F ~> C2 = λ[F ~> C2](apply(_))
}
