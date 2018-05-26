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

import slamdata.Predef.None
import quasar.common.PhaseResultT
import quasar.compile.SemanticErrors
import quasar.frontend.logicalplan.LogicalPlan
import quasar.fp._
import quasar.sql.Sql

import eu.timepit.refined.auto._
import matryoshka._
import matryoshka.data.Fix
import pathy.Path.rootDir
import scalaz._, Scalaz._

object Fixture {
  // TODO[scalaz]: Shadow the scalaz.Monad.monadMTMAB SI-2712 workaround
  import WriterT.writerTMonad

  def unsafeToLP(q: Fix[Sql], vars: Variables = Variables.empty): Fix[LogicalPlan] =
    compile.queryPlan[PhaseResultT[SemanticErrors \/ ?, ?], Fix, Fix[LogicalPlan]](q, vars, rootDir, 0L, None)
      .value.valueOr(err => scala.sys.error("Unexpected error compiling sql to LogicalPlan: " + err.shows))
}
