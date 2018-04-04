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

/** The Quasar API
  *
  * @tparam F the type of effects
  * @tparam G the type of streaming effects
  * @tparam Qry queries that may be executed
  * @tparam Cfg external datasource configuration
  * @tparam Stc static datasource content
  * @tparam Res query results
  * @tparam Cmp schema compression settings
  * @tparam Scm dataset schema
  */
trait Quasar[F[_], G[_], Qry, Cfg, Stc, Res, Cmp, Scm]
    extends DataSources[F, Cfg, Stc]
    with ResourceAccess[F, G]
    with QueryEvaluator[F, Qry, Res]
    with SchemaDiscovery[F, Qry, Cmp, Scm]

object Quasar {
  def apply[F[_], G[_], Qry, Cfg, Stc, Res, Cmp, Scm](
      dataSources: DataSources[F, Cfg, Stc],
      resourceAccess: ResourceAccess[F, G],
      queryEvaluator: QueryEvaluator[F, Qry, Res],
      schemaDiscovery: SchemaDiscovery[F, Qry, Cmp, Scm])
      : Quasar[F, G, Qry, Cfg, Stc, Res, Cmp, Scm] =
    new Quasar[F, G, Qry, Cfg, Stc, Res, Cmp, Scm] {
      // DataSources
      def createExternal(
          n: ResourceName,
          k: MediaType,
          c: Cfg,
          cr: ConflictResolution) =
        dataSources.createExternal(n, k, c, cr)

      def createStatic(
          n: ResourceName,
          sc: Stc,
          cr: ConflictResolution) =
        dataSources.createStatic(n, sc, cr)

      def lookup(n: ResourceName) =
        dataSources.lookup(n)

      def remove(n: ResourceName) =
        dataSources.remove(n)

      def rename(
          s: ResourceName,
          d: ResourceName,
          cr: ConflictResolution) =
        dataSources.rename(s, d, cr)

      def status =
        dataSources.status

      def supportedExternal =
        dataSources.supportedExternal

      // ResourceDiscovery
      def children(p: ResourcePath) =
        resourceAccess.children(p)

      def descendants(p: ResourcePath) =
        resourceAccess.descendants(p)

      def pathType(p: ResourcePath) =
        resourceAccess.pathType(p)

      // ResourceAccess
      def read(t: MediaType, p: ResourcePath) =
        resourceAccess.read(t, p)

      // QueryEvaluator
      def evaluate(q: Qry) =
        queryEvaluator.evaluate(q)

      // SchemaDiscovery
      def schema(q: Qry, cs: Cmp) = schemaDiscovery.schema(q, cs)
    }
}
