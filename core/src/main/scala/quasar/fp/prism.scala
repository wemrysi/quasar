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

import quasar.Predef.Unit

import monocle.Prism
import scalaz._

trait PrismInstances {
  import Liskov.<~<, Leibniz.===

  // TODO: See if we can implement this once for all tuples using shapeless.
  implicit class PrismOps[A, B](prism: Prism[A, B]) {
    def apply()(implicit ev: B === Unit): A =
      ev.subst[Prism[A, ?]](prism).reverseGet(())

    def apply(b: B): A =
      prism.reverseGet(b)

    def apply[C, D](c: C, d: D)(implicit ev: (C, D) <~< B): A =
      apply(ev((c, d)))

    def apply[C, D, E](c: C, d: D, e: E)(implicit ev: (C, D, E) <~< B): A =
      apply(ev((c, d, e)))

    def apply[C, D, E, F](c: C, d: D, e: E, f: F)(implicit ev: (C, D, E, F) <~< B): A =
      apply(ev((c, d, e, f)))

    def apply[C, D, E, F, G](c: C, d: D, e: E, f: F, g: G)(implicit ev: (C, D, E, F, G) <~< B): A =
      apply(ev((c, d, e, f, g)))

    def apply[C, D, E, F, G, H](c: C, d: D, e: E, f: F, g: G, h: H)(implicit ev: (C, D, E, F, G, H) <~< B): A =
      apply(ev((c, d, e, f, g, h)))
  }
}

object prism extends PrismInstances
