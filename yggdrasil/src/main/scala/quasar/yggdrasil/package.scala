/*
 * Copyright 2014–2018 SlamData Inc.
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

import quasar.contrib.scalaz.MonadTell_

package object yggdrasil {

  type Finalizers[F[_]] = List[F[Unit]]

  type MonadFinalizers[F[_], G[_]] = MonadTell_[F, Finalizers[G]]
  def MonadFinalizers[F[_], G[_]](implicit ev: MonadFinalizers[F, G]): MonadFinalizers[F, G] = ev
}
