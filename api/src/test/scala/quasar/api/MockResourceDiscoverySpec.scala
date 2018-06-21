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

import slamdata.Predef.Stream
import quasar.fp.reflNT

import scalaz.{Id, IList, Tree}, Id.Id

final class MockResourceDiscoverySpec extends ResourceDiscoverySpec[Id, IList] {
  val discovery =
    MockQueryEvaluator.resourceDiscovery[Id, IList](
      Stream(
        Tree.Node(
          ResourceName("a"),
          Stream(
            Tree.Leaf(ResourceName("b")),
            Tree.Node(
              ResourceName("c"),
              Stream(
                Tree.Leaf(ResourceName("d")))),
            Tree.Node(
              ResourceName("e"),
              Stream(
                Tree.Leaf(ResourceName("f")))))),
        Tree.Leaf(ResourceName("g")),
        Tree.Node(
          ResourceName("h"),
          Stream(
            Tree.Leaf(ResourceName("i")),
            Tree.Leaf(ResourceName("j"))))))

  val nonExistentPath = ResourcePath.root() / ResourceName("does") / ResourceName("not") / ResourceName("exist")

  val run = reflNT[Id]
}
