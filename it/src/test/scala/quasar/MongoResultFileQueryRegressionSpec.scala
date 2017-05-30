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

package quasar

import slamdata.Predef._

import quasar.common._
import quasar.contrib.pathy._
import quasar.fp.liftMT
import quasar.fs._
import quasar.fs.mount.ConnectionUri
import quasar.physical.mongodb._
import quasar.physical.mongodb.fs.MongoDbSpec._
import quasar.regression._
import quasar.sql.{Blob, Sql}

import matryoshka.data.Fix
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process

object MongoResultFileQueryRegressionSpec {
  def isSupported[S[_]](fs: SupportedFs[S]): Boolean =
    TestConfig.isMongo(fs.ref) && ResultFileQueryRegressionSpec.isSupported(fs)
}

class MongoResultFileQueryRegressionSpec
  extends QueryRegressionTest[FileSystemIO](
    QueryRegressionTest.externalFS.map(_.filter(MongoResultFileQueryRegressionSpec.isSupported))
  ) {

  val read = ReadFile.Ops[FileSystemIO]

  val suiteName = "MongoResultFile Queries"

  def queryResults(expr: Blob[Fix[Sql]], vars: Variables, basePath: ADir) = {
    import qfTransforms._

    type M[A] = FileSystemErrT[F, A]

    val hoistM: M ~> CompExecM =
      execToCompExec compose[M] Hoist[FileSystemErrT].hoist[F, G](liftMT[F, PhaseResultT])

    for {
      tmpFile <- hoistM(manage.tempFile(DataDir)).liftM[Process]
      outFile <- fsQ.executeQuery(expr, vars, basePath, tmpFile).liftM[Process]
      cleanup =  hoistM(
                   query.fileExists(tmpFile).liftM[FileSystemErrT].ifM(
                     manage.delete(tmpFile),
                     ().point[M])
                 ).whenM(outFile ≟ tmpFile)
      data    <- read.scanAll(outFile)
                   .translate(hoistM)
                   .onComplete(Process.eval_(cleanup))
    } yield data
  }

  override val TestsRoot = currentDir[Sandboxed] </> dir("it") </> dir("src") </> dir("main") </> dir("resources") </> dir("mongotests")

  override def loadData[S[_]](ref: BackendRef, dataLoc: RFile, testDir: ADir, run: Run): Task[Unit] =
    for {
      coll <- asColl(renameFile(testDir </> dir("regression") </> dataLoc, _.dropExtension))
      _ <- loadMongoData(ref, coll)
    } yield ()

  private def loadMongoData[S[_]](ref: BackendRef, coll: Collection): Task[Option[Unit]] = {
    def doInsert(uri: ConnectionUri) = connect(uri) map (client => insertInvalids(coll).run(client))

    val task = TestConfig.loadConnectionUri(ref).run map (_ map doInsert)
    task.unsafePerformSync.map(_.unsafePerformSync).sequence
  }

  private def nameValDoc(name: String, bson: Bson) = Bson.Doc(ListMap(
    "name" -> Bson.Text(name),
    "val" -> bson)
  )

  private def insertInvalids(coll: Collection) =
    MongoDbIO.insert(
      coll,
      List(
        nameValDoc("NaN",   Bson.Dec(Double.NaN)),
        nameValDoc("inf",   Bson.Dec(Double.PositiveInfinity)),
        nameValDoc("-inf",  Bson.Dec(Double.NegativeInfinity)),
        nameValDoc("valid", Bson.Dec(42.42)),
        nameValDoc("null",  Bson.Null)
      ).map(_.repr)
    )
}
