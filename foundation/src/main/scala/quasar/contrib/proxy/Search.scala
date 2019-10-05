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

package quasar.contrib.proxy

import cats.effect.Sync

import com.github.markusbernhardt.proxy.ProxySearch
import com.github.markusbernhardt.proxy.selector.misc.BufferedProxySelector.CacheScope
import com.github.markusbernhardt.proxy.util.PlatformUtil, PlatformUtil.Platform

import java.net.ProxySelector

object Search {

  def apply[F[_]: Sync]: F[ProxySelector] =
    Sync[F] delay {
      val builder = new ProxySearch

      if (PlatformUtil.getCurrentPlattform() == Platform.WIN) {
        builder.addStrategy(ProxySearch.Strategy.IE)
        builder.addStrategy(ProxySearch.Strategy.FIREFOX)
      } else if (PlatformUtil.getCurrentPlattform() == Platform.LINUX) {
        builder.addStrategy(ProxySearch.Strategy.GNOME)
        builder.addStrategy(ProxySearch.Strategy.KDE)
        builder.addStrategy(ProxySearch.Strategy.BROWSER)
      } else {
        // mac and other
        builder.addStrategy(ProxySearch.Strategy.OS_DEFAULT)    // TODO
      }

      // Cache 20 hosts for up to 10 minutes. This is the default
      builder.setPacCacheSettings(20, 1000 * 60 * 10, CacheScope.CACHE_SCOPE_HOST)

      builder.getProxySelector()
    }
}
