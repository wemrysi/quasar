package quasar
package physical
package mongodb
package fs

import quasar.Predef._
import quasar.config._
import quasar.fs._
import quasar.fp._
import quasar.regression._
import quasar.specs2._
import quasar.sql

import com.mongodb.MongoException

import monocle.Optional
import monocle.std.{disjunction => D}
import monocle.function.{Field1, Field2}
import monocle.std.tuple2._

import org.specs2.ScalaCheck
import org.specs2.execute.{AsResult, SkipException}

import pathy.Path._

import scalaz.{Optional => _, _}, Id._
import scalaz.stream._
import scalaz.std.vector._
import scalaz.syntax.monad._
import scalaz.syntax.show._
import scalaz.concurrent.Task

/** Unit tests for the MongoDB filesystem implementation. */
class MongoDbFileSystemSpec
  extends FileSystemTest[QueryFsIO](MongoDbFileSystemSpec.mongoFsUT)
  with ScalaCheck
  with ExclusiveExecution
  with SkippedOnUserEnv {

  import FileSystemTest._
  import FileSystemError._
  import ManageFile._
  import DataGen._

  val write  = WriteFile.Ops[QueryFsIO]
  val manage = ManageFile.Ops[QueryFsIO]
  val exec   = ExecutePlan.Ops[QueryFsIO]

  /** The test prefix from the config.
    *
    * NB: This is a bit brittle as we're assuming this is the correct source
    *     of configuration compatible with the supplied interpreters.
    */
  val testPrefix: Task[AbsDir[Sandboxed]] =
    TestConfig.testDataPrefix

  fileSystemShould { _ => implicit run =>
    "MongoDB" should {

      "Writing" >> {
        val invalidData = testPrefix.map(_ </> dir("invaliddata"))
                            .liftM[FileSystemErrT]

        "fail with `InvalidData` when attempting to save non-documents" ! prop {
          (data: Data, fname: Int) => isNotObj(data) ==> {
            val path = invalidData map (_ </> file(fname.toHexString))

            path.flatMap(p => runLogT(run, write.appendF[Id](p, data))).map { errs =>
              vectorFirst[FileSystemError]
                .composePrism(writeFailed)
                .composeLens(Field1.first)
                .isMatching(errs.toVector)
            }.run.run.toEither must beRight(true)
          }
        }

        step(invalidData.flatMap(p => runT(run)(manage.deleteDir(p))).runVoid)
      }

      /** NB: These tests effectively require "root" level permissions on the
        *     MongoDB server, but so does the functionality they exercise, so
        *     we're ok skipping them on an authorization error.
        */
      "Deletion" >> {
        type X[A] = Process[manage.M, A]

        val tmpDir: Task[AbsDir[Sandboxed]] =
          NameGenerator.salt map (s => rootDir </> dir(s))

        "top-level directory should delete database" >> {
          def check(d: AbsDir[Sandboxed])(implicit X: Apply[X]) = {
            val f = d </> file("deldb")

            (
              manage.ls(rootDir).liftM[Process]  |@|
              write.saveF(f, oneDoc).terminated  |@|
              manage.ls(rootDir).liftM[Process]  |@|
              manage.deleteDir(d).liftM[Process] |@|
              manage.ls(rootDir).liftM[Process]
            ) { (before, _, create, _, delete) =>
              val d0 = d.relativeTo(rootDir) getOrElse currentDir
              (before must not contain(Node.Dir(d0))) and
              (create must contain(Node.Dir(d0))) and
              (delete must_== before)
            }
          }

          tmpDir.flatMap(d =>
            rethrow[Task, FileSystemError]
              .apply(runLogT(run, check(d)))
              .handleWith(skipIfUnauthorized)
              .map(_.headOption getOrElse ko)
          ).run
        }

        "root dir should delete all databases" >> {
          def check(d1: AbsDir[Sandboxed], d2: AbsDir[Sandboxed])
                   (implicit X: Apply[X]) = {

            val f1 = d1 </> file("delall1")
            val f2 = d2 </> file("delall2")

            (
              write.saveF(f1, oneDoc).terminated       |@|
              write.saveF(f2, oneDoc).terminated       |@|
              manage.ls(rootDir).liftM[Process]        |@|
              manage.deleteDir(rootDir).liftM[Process] |@|
              manage.ls(rootDir).liftM[Process]
            ) { (_, _, before, _, after) =>
              val dA = d1.relativeTo(rootDir) getOrElse currentDir
              val dB = d2.relativeTo(rootDir) getOrElse currentDir

              (before must contain(Node.Dir(dA))) and
              (before must contain(Node.Dir(dB))) and
              (after must beEmpty)
            }
          }

          (tmpDir |@| tmpDir)((d1, d2) =>
            rethrow[Task, FileSystemError]
              .apply(runLogT(run, check(d1, d2)))
              .handleWith(skipIfUnauthorized)
              .map(_.headOption getOrElse ko)
          ).join.run
        }.skippedOnUserEnv("Would destroy user data.")
      }

      /** TODO: Testing this here closes the tests to the existence of
        *       `WorkflowExecutor`, but opens them to brittleness by assuming
        *       what is compiled to MR vs Aggregation. i.e. just because a
        *       query is compiled to MR now, doesn't mean it always will be.
        *
        *       Also, is this check something we should handle in `FileSystem`
        *       combinators? i.e. look through the LP provided to `ExecutePlan`
        *       and check that all the referenced files exist?
        *
        *       We also may want to change FileSystemUT[S[_]] to
        *       FileSystemUT[S[_], F[_]] to allow test suites to specify more
        *       granular constraints than just `Task`.
        */
      "Querying" >> {
        def shouldFailWithPathNotFound(f: String => String) = {
          val dne = testPrefix map (_ </> file("__DNE__"))
          val q = dne map (p => f(posixCodec.printPath(p)))
          val xform = ExecutePlan.Transforms[exec.F]
          val parser = new sql.SQLParser()

          import xform._

          val runExec: CompExecM ~> ExecErrT[PhaseResultT[Task, ?], ?] = {
            type X0[A] = PhaseResultT[Task, A]
            type X1[A] = ExecErrT[X0, A]

            val x0: G ~> X0 =
              Hoist[PhaseResultT].hoist(run)

            val x1: H ~> X0 =
              rethrow[X0, SemanticErrors].compose[H](Hoist[SemanticErrsT].hoist(x0))

            Hoist[ExecErrT].hoist(x1)
          }

          def check(file: AbsFile[Sandboxed]) = {
            val errP: Optional[ExecutionError \/ ResultFile, AbsFile[Sandboxed]] =
              D.left                   composePrism
              ExecutionError.pathError composeLens
              Field2.second            composePrism
              PathError2.pathNotFound  composePrism
              D.right

            def check0(expr: sql.Expr) =
              (run(manage.fileExists(file)).run must beFalse) and
              (errP.getOption(
                runExec(exec.executeQuery_(expr, Variables.fromMap(Map()))).run.value.run
              ) must beSome(file))

            parser.parse(sql.Query(f(posixCodec.printPath(file)))) fold (
              err => ko(s"Parsing failed: ${err.shows}"),
              check0)
          }

          dne.map(check).run
        }

        "mapReduce query should fail when file DNE" >> {
          shouldFailWithPathNotFound { path =>
            s"""SELECT name FROM "$path" WHERE LENGTH(name) > 10"""
          }
        }

        "aggregation query should fail when file DNE" >> {
          shouldFailWithPathNotFound { path =>
            s"""SELECT name FROM "$path" WHERE name.field1 > 10"""
          }
        }
      }
    }; ()
  }

  ////

  private def skipIfUnauthorized[A]: PartialFunction[Throwable, Task[A]] = {
    case ex: MongoException if ex.getMessage.contains("Command failed with error 13: 'not authorized on ") =>
      Task.fail(SkipException(skipped("No db-level permissions.")))
  }

  private def isNotObj: Data => Boolean = {
    case Data.Obj(_) => false
    case _           => true
  }
}

object MongoDbFileSystemSpec {
  // NB: No `chroot` here as we want to test deleting top-level
  //     dirs (i.e. databases).
  def mongoFsUT: Task[NonEmptyList[FileSystemUT[QueryFsIO]]] =
    TestConfig.externalFileSystems {
      case (MongoDbConfig(cs), dir) =>
        mongodb.filesystems.testQueryableFileSystem(cs, dir)
    }
}
