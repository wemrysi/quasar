package quasar.fs.mount

import quasar.Predef._
import quasar.EnvironmentError2
import quasar.effect._
import quasar.fp._
import quasar.fs.APath

import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

class MounterSpec extends MountingSpec[MountingF] {
  import MountConfig2._, MountingError._, MountRequest._

  type MEff[A]  = Coproduct[Task, MountConfigsF, A]

  val invalidUri = ConnectionUri(uriA.value + "INVALID")
  val invalidCfg = fileSystemConfig(dbType, invalidUri)
  val invalidErr = invalidConfig(invalidCfg, "invalid URI".wrapNel)

  val doMount: MountRequest => MountingError \/ Unit = {
    case MountFileSystem(_, `dbType`, `invalidUri`) => invalidErr.left
    case _                                          => ().right
  }

  def interpName = "Mounter"

  def interpret = {
    val mm = Mounter[Task, MEff](doMount.andThen(_.point[Task]), κ(Task.now(())))
    val cfgRef = TaskRef(Map.empty[APath, MountConfig2]).run

    val interpMnts: MountConfigsF ~> Task =
      Coyoneda.liftTF[MountConfigs, Task](KeyValueStore.fromTaskRef(cfgRef))

    val interpEff: MEff ~> Task =
      free.interpret2(NaturalTransformation.refl, interpMnts)

    val interp0: Mounting ~> Task =
      free.foldMapNT[MEff, Task](interpEff).compose(mm)

    Coyoneda.liftTF(interp0)
  }

  "Handling mounts" should {
    "fail when mount handler fails" >>* {
      val loc = rootDir </> dir("fs")
      val cfg = MountConfig2.fileSystemConfig(dbType, invalidUri)

      mnt.mountFileSystem(loc, dbType, invalidUri)
        .run.tuple(mnt.lookup(loc).run)
        .map(_ must_== ((MountingError.invalidConfig(cfg, "invalid URI".wrapNel).left, None)))
    }
  }
}
