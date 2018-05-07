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

import slamdata.Predef._
import quasar.api._, ResourceError._

import matryoshka.data.Fix
import scalaz.{Id, IMap, Show, Tree}, Id.Id
import scalaz.std.stream._
import scalaz.syntax.applicative._
import scalaz.syntax.either._

final class FederatingQueryEvaluatorSpec extends quasar.Qspec {

  implicit val showTree: Show[Tree[ResourceName]] =
    Show.shows(_.drawTree)

  val abForest =
    Stream(
      Tree.Node(ResourceName("b"), Stream(
        Tree.Leaf(ResourceName("a")))),
      Tree.Leaf(ResourceName("a")))

  val xyForest =
    Stream(
      Tree.Node(ResourceName("x"), Stream(
        Tree.Leaf(ResourceName("y")))),
      Tree.Node(ResourceName("y"), Stream(
        Tree.Leaf(ResourceName("y")))))

  val abs = MockQueryEvaluator.resourceDiscovery[Id](abForest)

  val xys = MockQueryEvaluator.resourceDiscovery[Id](xyForest)

  val qfed = new QueryFederation[Fix, Id, Int, Fix[QScriptFederated[Fix, Int, ?]]] {
    def evaluateFederated(q: Fix[QScriptFederated[Fix, Int, ?]]) = q.right[ReadError]
  }

  val fqe =
    FederatingQueryEvaluator(qfed, IMap(
      ResourceName("abs") -> ((abs, 1)),
      ResourceName("xys") -> ((xys, 2))).point[Id])

  "children" >> {
    "returns possible keys for root" >> {
      val progeny =
        IMap(
          ResourceName("abs") -> ResourcePathType.resourcePrefix,
          ResourceName("xys") -> ResourcePathType.resourcePrefix)

      fqe.children(ResourcePath.root()) must_= progeny.right
    }

    "returns PNF when no source" >> {
      val dne = ResourcePath.root() / ResourceName("foo") / ResourceName("bar")

      fqe.children(dne) must_= pathNotFound(dne).left
    }

    "returns correctly prefixed PNF from source" >> {
      val abx = ResourcePath.root() / ResourceName("abs") / ResourceName("a") / ResourceName("x")

      fqe.children(abx) must_= pathNotFound(abx).left
    }

    "returns results" >> {
      val xx = ResourcePath.root() / ResourceName("xys") / ResourceName("x")

      val expect = IMap(ResourceName("y") -> ResourcePathType.resource)

      fqe.children(xx) must_= expect.right
    }
  }

  "descendants" >> {
    "returns from all sources for root" >> {
      val expect = Stream(
        Tree.Node(ResourceName("abs"), abForest),
        Tree.Node(ResourceName("xys"), xyForest))

      fqe.descendants(ResourcePath.root()) must_= expect.right
    }

    "returns PNF when no source" >> {
      val dne = ResourcePath.root() / ResourceName("foo") / ResourceName("bar")

      fqe.descendants(dne) must_= pathNotFound(dne).left
    }

    "returns correctly prefixed PNF from source" >> {
      val yz = ResourcePath.root() / ResourceName("xys") / ResourceName("y") / ResourceName("z")

      fqe.descendants(yz) must_= pathNotFound(yz).left
    }

    "returns results" >> {
      fqe.descendants(ResourcePath.root() / ResourceName("abs")) must_= abForest.right
    }
  }

  "isResource" >> {
    "returns false for root" >> {
      fqe.isResource(ResourcePath.root()) must beFalse
    }

    "returns false when no source" >> {
      val noSource = ResourcePath.root() / ResourceName("baz") / ResourceName("bar")

      fqe.isResource(noSource) must beFalse
    }

    "returns false when not a resource" >> {
      val prefix = ResourcePath.root() / ResourceName("xys") / ResourceName("x")

      fqe.isResource(prefix) must beFalse
    }

    "returns true when a resource" >> {
      val resource = ResourcePath.root() / ResourceName("abs") / ResourceName("a")

      fqe.isResource(resource) must beTrue
    }
  }

  "evaluate" >> {
    "returns PNF when no source" >> todo

    "returns PNF when no source in branch" >> todo

    "builds federated qscript when all sources found" >> todo
  }
}
