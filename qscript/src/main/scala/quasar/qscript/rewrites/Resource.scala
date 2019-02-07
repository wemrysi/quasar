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

package quasar.qscript.rewrites

import slamdata.Predef.{List, Nil, Option}

import quasar.{IdStatus, ScalarStage}
import quasar.contrib.iota._
import quasar.qscript.{InterpretedRead, Read}

import scalaz.Const

object Resource {
  def apply[A] = new PartiallyApplied[A]

  final class PartiallyApplied[A] {
    def unapply[F[x] <: ACopK[x], B](fb: F[B])(
        implicit
        IR: Const[InterpretedRead[A], ?] :<<: F,
        R: Const[Read[A], ?] :<<: F)
        : Option[(A, IdStatus, List[ScalarStage])] = {

      def isIR = IR.prj(fb) map {
        case Const(InterpretedRead(a, ss)) => (a, ss.idStatus, ss.stages)
      }

      def isR = R.prj(fb) map {
        case Const(Read(a, idStatus)) => (a, idStatus, Nil)
      }

      isIR orElse isR
    }
  }
}
