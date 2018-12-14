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

package quasar.impl.datasource.local

import slamdata.Predef._

import cats.effect.IO

import fs2.Stream

import java.nio.file.Paths

import scala.concurrent.ExecutionContext

import quasar.{ParseInstruction, ParseType}
import quasar.api.resource.{ResourceName, ResourcePath}
import quasar.contrib.std.errorImpossible
import quasar.common.data.RValue
import quasar.common.{CPath, CPathField, CPathIndex}
import quasar.concurrent.BlockingContext
import quasar.connector.{Datasource, QueryResult, ResourceError, DatasourceSpec}
import quasar.contrib.scalaz.MonadError_
import quasar.qscript.InterpretedRead

import qdata.QData

import shims._

abstract class LocalDatasourceSpec
    extends DatasourceSpec[IO, Stream[IO, ?]] {

  implicit val ioMonadResourceErr: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  implicit val tmr = IO.timer(ExecutionContext.Implicits.global)

  override def datasource
      : Datasource[IO, Stream[IO, ?], InterpretedRead[ResourcePath], QueryResult[IO]]

  val nonExistentPath =
    ResourcePath.root() / ResourceName("non") / ResourceName("existent")

  def gatherMultiple[A](g: Stream[IO, A]) = g.compile.toList

  "listing a file path returns none" >>* {
    datasource
      .prefixedChildPaths(ResourcePath.root() / ResourceName("smallZips.data"))
      .map(_ must beNone)
  }

  "returns data from a nonempty file" >>* {
    datasource
      .evaluate(InterpretedRead(ResourcePath.root() / ResourceName("smallZips.data"), List()))
      .flatMap(_.data.compile.fold(0)((c, _) => c + 1))
      .map(_ must be_>(0))
  }
}

object LocalDatasourceSpec extends LocalDatasourceSpec {
  val blockingPool = BlockingContext.cached("local-datasource-spec")

  def datasource =
    LocalDatasource[IO](Paths.get("./it/src/main/resources/tests"), 1024, blockingPool)
}

object LocalParsedDatasourceSpec extends LocalDatasourceSpec {
  val blockingPool = BlockingContext.cached("local-parsed-datasource-spec")

  def datasource =
    LocalParsedDatasource[IO, RValue](Paths.get("./it/src/main/resources/tests"), 1024, blockingPool)
}

trait LocalInterpretedReadDatasourceSpec extends LocalDatasourceSpec {
  val interpreter: LocalInterpretedReadDatasource.Interpreter[IO, RValue]
  val blockingPool: BlockingContext

  def datasource =
    LocalInterpretedReadDatasource[IO, RValue](
      Paths.get("./it/src/main/resources/tests"),
      1024,
      blockingPool,
      interpreter
    )

  "interpretedRead works correctly" >>* {
    val loc = ParseInstruction.Project(CPath(List(CPathField("loc"))))
    val mask = ParseInstruction.Mask(Map(CPath(List(CPathIndex(0))) -> Set(ParseType.Number)))
    val index0 = ParseInstruction.Project(CPath(List(CPathIndex(0))))
    val instructions = List(loc, mask, index0)
    val resource = ResourcePath.root() / ResourceName("smallZips.data")
    val iRead = InterpretedRead(resource, instructions)
    datasource.evaluate(iRead) flatMap { _ match {
      case QueryResult.Parsed(decode, data, instructions) =>
        val rValueStream = data.map(x => QData.convert(x)(decode, RValue.qdataEncode))
        LocalInterpretedReadDatasource
          .interpretStream[IO, RValue](rValueStream, LocalInterpretedReadDatasource.fullInterpreter[IO, RValue], instructions)
          .compile.toList
          .map(x => x.length === 100)
      case _ => errorImpossible
    }}
  }
}

object LocalInterpretedReadDatasourceFullInterpretSpec extends LocalInterpretedReadDatasourceSpec {
  val blockingPool = BlockingContext.cached("local-interpreted-datasource-spec")
  val interpreter = LocalInterpretedReadDatasource.fullInterpreter[IO, RValue]
}

object LocalInterpretedReadDatasourceMaskInterpretSpec extends LocalInterpretedReadDatasourceSpec {
  val blockingPool = BlockingContext.cached("local-interpreted-datasource-spec-mask")
  val interpreter = LocalInterpretedReadDatasource.onlyMaskInterpreter[IO, RValue]
}

object LocalInterpretedReadDatasourceNoMaskInterpretSpec extends LocalInterpretedReadDatasourceSpec {
  val blockingPool = BlockingContext.cached("local-interpreted-datasource-spec-no-mask")
  val interpreter = LocalInterpretedReadDatasource.noMaskInterpreter[IO, RValue]
}

object LocalInterpretedReadDatasourceNoInterpretSpec extends LocalInterpretedReadDatasourceSpec {
  val blockingPool = BlockingContext.cached("local-interpreted-datasource-spec-no")
  val interpreter = LocalInterpretedReadDatasource.noInterpreter[IO, RValue]
}
