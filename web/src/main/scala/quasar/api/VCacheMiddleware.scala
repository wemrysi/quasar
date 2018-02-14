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

package quasar.api

import quasar.effect.Timing
import quasar.fs.mount.cache.VCache, VCache.VCacheExpR

import scalaz._

object VCacheMiddleware {
  def apply[S[_]](
    service: QHttpService[S]
  )(implicit
    R: VCacheExpR.Ops[S],
    T: Timing.Ops[S],
    C: Catchable[Free[S, ?]]
  ): QHttpService[S] =
    //Due to qz-3657 cache specific response headers have been removed for the moment.
    //However, since the plan is to reintroduce them in the form of ETags,
    //we are keeping the infrastructure
    QHttpService { case req => service(req) }
}
