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

package quasar.physical.sparkcore.fs.hdfs

import quasar.Predef._
import quasar.Data
import quasar.physical.sparkcore.fs.queryfile.Input
import quasar.contrib.pathy._
import quasar.fs.FileSystemError
import quasar.fs.FileSystemError._
import quasar.fs.PathError._
import quasar.contrib.pathy._

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import org.apache.spark._
import org.apache.spark.rdd._


object queryfile {

  private def toPath(apath: APath): Task[Path] = Task.delay {
    new Path(posixCodec.unsafePrintPath(apath))
  }

  def fromFile(sc: SparkContext, file: AFile): Task[RDD[String]] = ???

  def store(rdd: RDD[Data], out: AFile): Task[Unit] = ???

  def fileExists(fileSystem: () => FileSystem)(f: AFile): Task[Boolean] =
    toPath(f).map(path => {
      val hdfs = fileSystem()
      val exists = hdfs.exists(path)
      hdfs.close()
      exists
    })

  def listContents(fileSystem:() => FileSystem)(d: ADir): EitherT[Task, FileSystemError, Set[PathSegment]] =
    EitherT(toPath(d).map(path => {
      val hdfs = fileSystem()
      val result = if(hdfs.exists(path)) {
        hdfs.listStatus(path).toSet.map {
          case file if file.isFile() => FileName(file.getPath().getName()).right[DirName]
          case directory => DirName(directory.getPath().getName()).left[FileName]
        }.right[FileSystemError]
      } else pathErr(pathNotFound(d)).left[Set[PathSegment]]
      hdfs.close
      result
    }))

  def readChunkSize: Int = 5000

  def input(fileSystem: () => FileSystem): Input =
    Input(fromFile _, store _, fileExists(fileSystem) _, listContents(fileSystem) _, readChunkSize _)
}
