package quasar.fs.mount

import quasar.Predef.{implicitly, String}
import quasar.fp.{free, TaskRef, liftMT}
import quasar.fs.{FileSystem, FileSystemType, PathError2, ADir}
import quasar.effect._

import monocle.function.Field1
import monocle.std.tuple2._
import pathy.Path._
import org.specs2.mutable
import scalaz.{Failure => _, _}
import scalaz.concurrent.Task
import scalaz.syntax.applicative._

class FileSystemMounterSpec extends mutable.Specification {
  import FileSystemDef._

  type Abort[A]  = Failure[String, A]
  type AbortF[A] = Coyoneda[Abort, A]
  type AbortM[A] = Free[AbortF, A]

  type ResMnts = Mounts[DefinitionResult[AbortM]]

  type MountedFs[A]  = AtomicRef[ResMnts, A]
  type MountedFsF[A] = Coyoneda[MountedFs, A]

  type Eff0[A] = Coproduct[AbortF, MountedFsF, A]
  type Eff[A]  = Coproduct[AbortM, Eff0, A]
  type EffM[A] = Free[Eff, A]

  type TaskR[A] = Task[(ResMnts, String \/ A)]

  val abort = Failure.Ops[String, AbortF](implicitly, implicitly[AbortF :<: AbortF])

  def eval(rms: ResMnts): EffM ~> TaskR =
    new (EffM ~> TaskR) {
      type MT[F[_], A] = EitherT[F, String, A]
      type M[A] = MT[Task, A]

      val evalAbort: AbortF ~> M =
        Coyoneda.liftTF[Abort, M](Failure.toEitherT[Task, String])

      def apply[A](ma: EffM[A]) = TaskRef(rms) flatMap { ref =>
        val evalMnts: MountedFsF ~> Task =
          Coyoneda.liftTF[MountedFs, Task](AtomicRef.fromTaskRef(ref))

        val evalEff: Eff ~> M =
          free.interpret3[AbortM, AbortF, MountedFsF, M](
            free.foldMapNT(evalAbort),
            evalAbort,
            liftMT[Task, MT] compose evalMnts)

        (ma.foldMap(evalEff).run |@| ref.read)((r, mnts) => (mnts, r))
      }
    }


  val abortFs: FileSystem ~> AbortM =
    new (FileSystem ~> AbortM) {
      def apply[A](fs: FileSystem[A]) =
        abort.fail("FileSystem")
    }

  def fsResult(sig: String): DefinitionResult[AbortM] =
    (abortFs, abort.fail(sig))

  val fsDef = FileSystemDef.fromPF {
    case (typ, _) if typ == testType =>
      type X[A] = DefErrT[AbortM, A]
      fsResult("DEF").point[X]
  }

  val invalidPath =
    MountingError.pathError composePrism
    PathError2.invalidPath  composeLens
    Field1.first

  val fsMounter = FileSystemMounter(fsDef)
  val testType = FileSystemType("test")
  val testUri = ConnectionUri("https://test.example.com")
  def mount(d: ADir) = fsMounter.mount[Eff](d, testType, testUri)
  val unmount = fsMounter.unmount[Eff] _

  "FileSystemMounter" should {
    "mounting" >> {
      "fails when mounting above an exsiting mount" >> {
        val d1 = rootDir </> dir("foo")
        val d2 = d1 </> dir("bar")

        eval(Mounts.singleton(d2, fsResult("A")))(mount(d1)).run._2 must beLike {
          case \/-(-\/(err)) => invalidPath.getOption(err) must beSome(d1)
        }
      }

      "fails when mounting below an existing mount" >> {
        val d1 = rootDir </> dir("foo")
        val d2 = d1 </> dir("bar")

        eval(Mounts.singleton(d1, fsResult("B")))(mount(d2)).run._2 must beLike {
          case \/-(-\/(err)) => invalidPath.getOption(err) must beSome(d2)
        }
      }

      "fails when filesystem creation fails" >> {
        val d = rootDir </> dir("create") </> dir("fails")
        val fsd = Monoid[FileSystemDef[AbortM]].zero
        val fsm = FileSystemMounter(fsd)

        eval(Mounts.empty)(fsm.mount[Eff](d, testType, testUri))
          .run._2 must beLike {
            case \/-(-\/(MountingError.InvalidConfig(_, _))) => ok
          }
      }

      "stores the interpreter and cleanup effect when successful" >> {
        val d = rootDir </> dir("success")
        eval(Mounts.empty)(mount(d)).run._1.toMap.isEmpty must beFalse
      }

      "cleans up previous filesystem when mount is replaced" >> {
        val d = rootDir </> dir("replace") </> dir("cleanup")
        val cln = "CLEAN"
        val (rmnts, signal) = eval(Mounts.singleton(d, fsResult(cln)))(mount(d)).run

        (rmnts.toMap.isEmpty must beFalse) and (signal must_== \/.left(cln))
      }
    }

    "unmounting" >> {
      "cleanup a filesystem when unmounted" >> {
        val d = rootDir </> dir("unmount") </> dir("cleanup")
        val undo = "UNDO"

        eval(Mounts.singleton(d, fsResult(undo)))(unmount(d))
          .run._2 must_== \/.left(undo)
      }

      "deletes the entry from mounts" >> {
        val d = rootDir </> dir("unmount") </> dir("deletes")

        eval(Mounts.singleton(d, fsResult("C")))(unmount(d))
          .run._1.toMap.isEmpty must beTrue
      }
    }
  }
}
