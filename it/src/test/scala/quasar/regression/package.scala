package quasar

import quasar.Predef.{Long, Map}
import quasar.fs.{QueryFile, FileSystem, ADir}
import quasar.fp.{free, TaskRef}
import quasar.effect._

import scalaz.{Failure => _, _}
import scalaz.syntax.apply._
import scalaz.concurrent._

package object regression {
  import quasar.fs.mount.hierarchical.{HFSFailureF, MountedResultHF}

  type FileSystemIO[A] = Coproduct[Task, FileSystem, A]

  type HfsIO0[A] = Coproduct[MountedResultHF, Task, A]
  type HfsIO1[A] = Coproduct[HFSFailureF, HfsIO0, A]
  type HfsIO[A]  = Coproduct[MonotonicSeqF, HfsIO1, A]

  val interpretHfsIO: Task[HfsIO ~> Task] = {
    import QueryFile.ResultHandle
    import quasar.fs.mount.hierarchical._

    def handlesTask(
      ref: TaskRef[Map[ResultHandle, (ADir, ResultHandle)]]
    ) : MountedResultHF ~> Task =
      Coyoneda.liftTF[MountedResultH, Task](
        KeyValueStore.fromTaskRef(ref))

    def monoSeqTask(ref: TaskRef[Long]): MonotonicSeqF ~> Task =
      Coyoneda.liftTF[MonotonicSeq, Task](
        MonotonicSeq.taskRefMonotonicSeq(ref))

    val hfsFailTask: HFSFailureF ~> Task =
      Coyoneda.liftTF[HFSFailure, Task](
        Failure.toTaskFailure[HierarchicalFileSystemError])

    (TaskRef(Map.empty[ResultHandle, (ADir, ResultHandle)]) |@| TaskRef(0L))(
      (handles, ct) => free.interpret4(
        monoSeqTask(ct),
        hfsFailTask,
        handlesTask(handles),
        NaturalTransformation.refl))
  }
}
