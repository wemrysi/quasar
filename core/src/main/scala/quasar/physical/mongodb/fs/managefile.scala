package quasar
package physical
package mongodb
package fs

import quasar.Predef._
import quasar.fs._

import com.mongodb.MongoServerException
import com.mongodb.async.client.MongoClient
import scalaz._, Scalaz._
import scalaz.stream._
import scalaz.concurrent.Task
import pathy.Path._

object managefile {
  import ManageFile._, FileSystemError._, PathError2._, MongoDb._

  type GenState              = (String, Long)
  type ManageStateT[F[_], A] = StateT[F, GenState, A]
  type ManageMongo[A]        = ManageStateT[MongoDb, A]

  /** TODO: There are still some questions regarding Path
    *   1) We should assume all paths will be canonicalized and can do so
    *      with a ManageFile ~> ManageFile that canonicalizes everything.
    *
    *   2) Currently, parsing a directory like "/../foo/bar/" as an absolute
    *      dir succeeds, this should probably be changed to fail.
    */

  /** Interpret [[ManageFile]] into [[ManageMongo]], given the name of a database
    * to use as a default location for temp collections when no other database
    * can be deduced.
    */
  def interpret(defaultDb: String): ManageFile ~> ManageMongo = new (ManageFile ~> ManageMongo) {
    def apply[A](fs: ManageFile[A]) = fs match {
      case Move(scenario, semantics) =>
        scenario.fold(moveDir(_, _, semantics), moveFile(_, _, semantics))
          .run.liftM[ManageStateT]

      case Delete(path) =>
        path.fold(deleteDir, deleteFile)
          .run.liftM[ManageStateT]

      case ListContents(dir) =>
        (dirName(dir) match {
          case Some(_) =>
            collectionsInDir(dir).map(_.flatMap(c =>
              c.asFile flatMap (_ relativeTo rootDir) map Node.File
            ).toSet).run

          case None if depth(dir) == 0 =>
            collections
              .map(_.asFile flatMap (_ relativeTo rootDir) map Node.File)
              .pipe(process1.stripNone)
              .runLog
              .map(_.toSet.right[FileSystemError])

          case None =>
            nonExistentParent[Set[Node]](dir).run
        }).liftM[ManageStateT]

      case TempFile(maybeNear) =>
        val dbName = maybeNear
          .flatMap(f => Collection.fromFile(f).toOption)
          .cata(_.databaseName, defaultDb)

        freshName map (n => rootDir </> dir(dbName) </> file(n))
    }
  }

  def run(client: MongoClient): ManageMongo ~> Task =
    new (ManageMongo ~> Task) {
      def apply[A](fs: ManageMongo[A]) =
        Task.delay(scala.util.Random.nextInt().toHexString)
          .map(s => s"__quasar.tmp_${s}_")
          .flatMap(p => fs.eval((p, 0)).run(client))
    }

  ////

  private type M[A] = FileSystemErrT[MongoDb, A]
  private type G[E, A] = EitherT[MongoDb, E, A]

  private val prefixL: GenState @> String =
    Lens.firstLens

  private val seqL: GenState @> Long =
    Lens.secondLens

  private def moveToRename(sem: MoveSemantics): RenameSemantics = {
    import RenameSemantics._
    sem.fold(Overwrite, FailIfExists, Overwrite)
  }

  private def moveDir(src: AbsDir[Sandboxed], dst: AbsDir[Sandboxed], sem: MoveSemantics)
                     : M[Unit] = {
    for {
      colls    <- collectionsInDir(src)
      srcFiles <- if (colls.nonEmpty) colls.flatMap(_.asFile).point[M]
                  else MonadError[G, FileSystemError].raiseError(PathError(DirNotFound(src)))
      dstFiles =  srcFiles flatMap (_ relativeTo (src) map (dst </> _))
      _        <- srcFiles zip dstFiles traverseU {
                    case (s, d) => moveFile(s, d, sem)
                  }
    } yield ()
  }

  private def moveFile(src: AbsFile[Sandboxed], dst: AbsFile[Sandboxed], sem: MoveSemantics)
                      : M[Unit] = {
    /** Error codes obtained from MongoDB `renameCollection` docs:
      * See http://docs.mongodb.org/manual/reference/command/renameCollection/
      */
    def reifyMongoErr(m: MongoDb[Unit]): M[Unit] =
      EitherT(m.attempt flatMap {
        case -\/(e: MongoServerException) if e.getCode == 10026 =>
          PathError(FileNotFound(src)).left.point[MongoDb]

        case -\/(e: MongoServerException) if e.getCode == 10027 =>
          PathError(FileExists(dst)).left.point[MongoDb]

        case -\/(t) =>
          fail(t)

        case \/-(_) =>
          ().right.point[MongoDb]
      })

    def ensureDstExists(dstColl: Collection): M[Unit] =
      EitherT(collectionsIn(dstColl.databaseName)
                .filter(_ == dstColl)
                .runLast
                .map(_.toRightDisjunction(PathError(FileNotFound(dst))).void))

    for {
      srcColl <- collFromFileM(src)
      dstColl <- collFromFileM(dst)
      rSem    =  moveToRename(sem)
      _       <- sem.fold(().point[M], ().point[M], ensureDstExists(dstColl))
      _       <- reifyMongoErr(rename(srcColl, dstColl, rSem))
    } yield ()
  }

  // TODO: Really need a Path#fold[A] method, which will be much more reliable
  //       than this process of deduction.
  private def deleteDir(dir: AbsDir[Sandboxed]): M[Unit] =
    dirName(dir) match {
      case Some(n) if depth(dir) == 1 =>
        dropDatabase(n.value).liftM[FileSystemErrT]

      case Some(_) =>
        collectionsInDir(dir)
          .flatMap(_.traverseU_(c => dropCollection(c).liftM[FileSystemErrT]))

      case None if depth(dir) == 0 =>
        dropAllDatabases.liftM[FileSystemErrT]

      case None =>
        nonExistentParent(dir)
    }

  private def deleteFile(file: AbsFile[Sandboxed]): M[Unit] =
    collFromFileM(file) flatMap (c => dropCollection(c).liftM[FileSystemErrT])

  private def collectionsInDir(dir: AbsDir[Sandboxed]): M[Vector[Collection]] =
    collFromDirM(dir) flatMap (c =>
      collectionsIn(c.databaseName)
        .filter(_.collectionName startsWith c.collectionName)
        .runLog.map(_.toVector).liftM[FileSystemErrT])

  private def collFromDirM(dir: AbsDir[Sandboxed]): M[Collection] =
    EitherT(Collection.fromDir(dir).leftMap(PathError).point[MongoDb])

  private def collFromFileM(file: AbsFile[Sandboxed]): M[Collection] =
    EitherT(Collection.fromFile(file).leftMap(PathError).point[MongoDb])

  // TODO: This would be eliminated if we switched to AbsDir everywhere and
  //       disallowed AbsDirs like "/../foo" by construction.
  private def nonExistentParent[A](dir: AbsDir[Sandboxed]): M[A] =
    PathError(InvalidPath(dir.left, "directory refers to nonexistent parent"))
      .raiseError[G, A]

  private def freshName: ManageMongo[String] =
    (prefixL.st |@| seqL.modo(_ + 1))(_ + _).lift[MongoDb]
}
