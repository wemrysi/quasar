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

package quasar.physical.fallback.fs

import quasar.Predef._
import scalaz._
import com.precog.mimir._
import com.precog._, yggdrasil._, vfs._, table._
import com.precog.common._, security._

object fall {
  object stack extends StdLibEvaluatorStack[Need] {
    import trans.TransSpec1

    type M[+A] = Need[A]
    val M      = implicitly[Monad[Need]]

    object vfs extends VFSMetadata[Need] {
      def findDirectChildren(apiKey: APIKey, path: Path): EitherT[M, ResourceError, Set[PathMetadata]]                           = ???
      def pathStructure(apiKey: APIKey, path: Path, property: CPath, version: Version): EitherT[M, ResourceError, PathStructure] = ???
      def size(apiKey: APIKey, path: Path, version: Version): EitherT[M, ResourceError, Long]                                    = ???
    }

    trait TableCompanion extends ColumnarTableCompanion

    object Table extends TableCompanion {
      def apply(slices: StreamT[M, Slice], size: TableSize): Table                                                    = ???
      def singleton(slice: Slice): Table                                                                              = ???
      def align(sourceLeft: Table, alignOnL: TransSpec1, sourceRight: Table, alignOnR: TransSpec1): M[(Table, Table)] = ???
    }
  }
}
