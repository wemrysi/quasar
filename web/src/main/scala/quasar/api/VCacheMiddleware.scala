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

package quasar.api

import slamdata.Predef._
import quasar.effect.Timing
import quasar.fs.mount.cache.VCache, VCache.VCacheExpR

import java.time.Instant
import org.http4s.{Header, HttpDate}
import org.http4s.headers.Expires
import scalaz._, Scalaz._
import scalaz.syntax.tag._

object VCacheMiddleware {
  private val minHttpDate: Instant = HttpDate.MinValue.toInstant
  private val maxHttpDate: Instant = HttpDate.MaxValue.toInstant

  def validDate(date: Instant) =
    if (date.isBefore(minHttpDate))
      minHttpDate
    else if (date.isAfter(maxHttpDate))
      maxHttpDate
    else date

  def apply[S[_]](
    service: QHttpService[S]
  )(implicit
    R: VCacheExpR.Ops[S],
    T: Timing.Ops[S],
    C: Catchable[Free[S, ?]]
  ): QHttpService[S] =
    QHttpService { case req =>
      (service(req) ⊛ R.ask ⊛ T.timestamp) { case (resp, ex, ts) =>
        val cacheHeaders = ex.unwrap.foldMap[List[Header]] { e =>
          val date = e.v.toInstant

          (Expires(HttpDate.unsafeFromInstant(validDate(date))): Header) ::
            ts.isAfter(date).fold(List(StaleHeader), Nil)
        }

        resp.modifyHeaders(_ ++ cacheHeaders)
      }
    }
}
