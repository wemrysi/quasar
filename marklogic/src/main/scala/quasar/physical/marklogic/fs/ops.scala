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

package quasar.physical.marklogic.fs

import quasar.Predef._
import quasar.contrib.pathy._
import quasar.contrib.scalaz._
import quasar.effect.uuid._
import quasar.fp.numeric._
import quasar.physical.marklogic.ErrorMessages
import quasar.physical.marklogic.qscript.StructuralPlanner
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._
import quasar.physical.marklogic.xcc._, Xcc.ops._

import com.marklogic.xcc.types.{XdmItem, XSBoolean, XSString}
import eu.timepit.refined.auto._
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.stream.Process

object ops {
  import expr.func

  private type W[A] = Writer[Prologs, A]

  /** Appends the given contents to the file, which must already exist. */
  def appendToFile[F[_]: Monad: Xcc: UuidReader: PrologW: PrologL, FMT, A](
    file: AFile,
    contents: A
  )(implicit
    C:  AsContent[FMT, A],
    SP: StructuralPlanner[F, FMT]
  ): F[ErrorMessages \/ Vector[XccError]] = {
    def appendQuery(src: AFile): F[MainModule] =
      MainModule fromWritten {
        SP.leftShift(lib.rootNode(src))
          .flatMap(lib.appendChildNodes[F, FMT].apply(lib.rootNode(file), _))
          .strengthL(Version.`1.0-ml`)
      }

    val tmpFile = UuidReader[F] asks { uuid =>
      renameFile(file, _ changeExtension (ext => s"$ext.$uuid"))
    }

    Xcc[F].transact(for {
      tmp  <- tmpFile
      errs <- createFile[F, FMT, A](tmp, contents)
      main <- appendQuery(tmp)
      _    <- Xcc[F].execute(main)
      _    <- deleteFile(tmp)
    } yield errs)
  }

  /** Returns whether the server is configured to create directories automatically. */
  def automaticDirectoryCreationEnabled[F[_]: Functor: Xcc]: F[Boolean] = {
    val main = MainModule fromWritten {
      admin.getConfiguration[W]
        .flatMap(admin.databaseGetDirectoryCreation[W](_, xdmp.database()))
        .strengthL(Version.`1.0-ml`)
    }

    Xcc[F].results(main.value) map {
      case Vector(createMode: XSString) => createMode.asString === "automatic"
      case _                            => false
    }
  }

  /** Creates a new file containing the given contents, overwriting any
    * existing file. Returns any errors encountered, either with the
    * contents or during the process of creation itself.
    */
  def createFile[F[_]: Monad: Xcc, FMT, A](
    file: AFile,
    contents: A
  )(implicit
    C: AsContent[FMT, A]
  ): F[ErrorMessages \/ Vector[XccError]] = {
    val uri         = pathUri(file)
    val contentUri  = ContentUri.getOption(uri) \/> s"Malformed content URI: $uri".wrapNel
    val content     = contentUri >>= (C.asContent[ErrorMessages \/ ?](_, contents))

    EitherT.fromDisjunction[F](content)
      .flatMapF(c => Xcc[F].insert[Id](c) map (_.right[ErrorMessages]))
      .run
  }

  /** Deletes the given directory and all descendants, recursively. */
  def deleteDir[F[_]: Xcc](dir: ADir): F[Executed] =
    Xcc[F].executeQuery(xdmp.directoryDelete(pathUri(dir).xs))

  /** Deletes the given file, erroring if it doesn't exist. */
  def deleteFile[F[_]: Xcc](file: AFile): F[Executed] =
    Xcc[F].executeQuery(xdmp.documentDelete(pathUri(file).xs))

  /** The set of child directories and files of the given directory. */
  def directoryContents[F[_]: Functor: Xcc](dir: ADir): F[Set[PathSegment]] = {
    def parseDir(s: String): Option[PathSegment] =
      UriPathCodec.parseAbsDir(s) flatMap dirName map (_.left)

    def parseFile(s: String): Option[PathSegment] =
      UriPathCodec.parseAbsFile(s) map (f => fileName(f).right)

    val main = MainModule.fromWritten(
      lib.directoryContents[W].apply(pathUri(dir).xs) strengthL Version.`1.0-ml`
    ).value

    Xcc[F].results(main) map (_ foldMap {
      case item: XSString =>
        val str = item.asString
        (parseDir(str) orElse parseFile(str)).toSet

      case _ => Set()
    })
  }

  /** Ensure the given directory exists, creating it if necessary. */
  def ensureDirectory[F[_]: Monad: Xcc](dir: ADir): F[Executed] = {
    import XccError.Code
    Xcc[F].executeQuery(xdmp.directoryCreate(pathUri(dir).xs)) handle {
      case XccError.QueryError(_, c)
             if Code.string.getOption(c.getCode).exists(_ === Code.DirExists) =>
        Executed.executed
    }
  }

  /** Ensure the given directory and all ancestors exist, creating them if necessary. */
  def ensureLineage[F[_]: Monad: Xcc](dir: ADir): F[Executed] = {
    def ensureLineage0(d: ADir): F[Executed] =
      ensureDirectory[F](d) <* parentDir(d).traverse(ensureLineage0)

    automaticDirectoryCreationEnabled[F]
      .ifM(ensureDirectory[F](dir), ensureLineage0(dir))
      .transact
  }

  /** Returns whether the given path exists. */
  def exists[F[_]: Monad: Xcc](path: APath): F[Boolean] = {
    def dirExists(d: ADir): XQuery   = fn.exists(cts.uriMatch(s"${pathUri(d)}*".xs, IList()))
    def fileExists(f: AFile): XQuery = fn.docAvailable(pathUri(f).xs)

    Xcc[F].queryResults(refineType(path).fold(dirExists, fileExists)) map {
      case Vector(b: XSBoolean) => b.asPrimitiveBoolean
      case _                    => false
    }
  }

  /** Move `src` to `dst` overwriting any existing contents. */
  def moveDir[F[_]: Monad: Xcc: PrologW: PrologL](src: ADir, dst: ADir): F[Executed] = {
    val decodeDir: XdmItem => Option[ADir] = {
      case str: XSString => UriPathCodec.parseAbsDir(str.asString) map (sandboxAbs(_))
      case _             => None
    }

    val dstDirs =
      lib.copyDirectory[F].apply(pathUri(src).xs, pathUri(dst).xs) map { uris =>
        val u = $("u")
        fn.distinctValues(fn.map(func(u.render) {
          fn.stringJoin(
            mkSeq_(fn.tokenize(~u, "/".xs)(1.xqy to mkSeq_(fn.last - 1.xqy)), "".xs),
            "/".xs)
        }, uris))
      }

    if (src === dst)
      Executed.executed.point[F]
    else
      Xcc[F].transact(for {
        query  <- MainModule.fromWritten(dstDirs strengthL Version.`1.0-ml`)
        items  <- Xcc[F].results(query)
        dirs   =  items.map(decodeDir).unite
        _      <- dirs traverse_ (ensureLineage[F](_).void)
        exec   <- deleteDir[F](src)
      } yield exec)
  }

  /** Move `src` to `dst` overwriting any existing contents. */
  def moveFile[F[_]: Monad: Xcc](src: AFile, dst: AFile): F[Executed] = {
    def insertDst = Xcc[F] executeQuery {
      xdmp.documentInsert(pathUri(dst).xs, fn.doc(pathUri(src).xs))
    }

    if (src === dst)
      Executed.executed.point[F]
    else
      exists[F](src)
        .flatMap(_ whenM (insertDst *> deleteFile(src)))
        .transact
        .as(Executed.executed)
  }

  /** Stream of the left-shifted contents of the given file. */
  def readFile[F[_]: Monad: Xcc: PrologL, FMT](
    file: AFile,
    offset: Natural,
    limit: Option[Positive]
  )(implicit
    SP: StructuralPlanner[F, FMT]
  ): Process[F, XdmItem] = {
    val query = SP.leftShift(lib.rootNode(file)) map { items =>
      (offset.get + 1, limit.map(_.get.xqy)) match {
        case (1, None) => items
        case (o, l)    => fn.subsequence(items, o.xqy, l)
      }
    }

    MainModule.fromWritten(query strengthL Version.`1.0-ml`)
      .liftM[Process]
      .flatMap(Xcc[F].evaluate)
  }

  /** Appends contents to an existing file, creating it otherwise. */
  def upsertFile[F[_]: Monad: Xcc: UuidReader: PrologW: PrologL, FMT, A](
    file: AFile,
    contents: A
  )(implicit
    C:  AsContent[FMT, A],
    SP: StructuralPlanner[F, FMT]
  ): F[ErrorMessages \/ Vector[XccError]] =
    exists[F](file).ifM(
      appendToFile[F, FMT, A](file, contents),
      createFile[F, FMT, A](file, contents))
}
