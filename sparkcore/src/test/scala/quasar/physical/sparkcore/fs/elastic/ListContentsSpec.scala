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

package quasar.physical.sparkcore.fs.elastic

import slamdata.Predef._
import quasar.contrib.pathy._
import quasar.fs.FileSystemError

import pathy._, Path._
import scalaz._, Scalaz._
import org.specs2.scalaz.DisjunctionMatchers

class ListContentsSpec extends quasar.Qspec with DisjunctionMatchers {

  def elasticInterpreter(indices: List[String]): ElasticCall ~> Id = new (ElasticCall ~> Id) {
    def apply[A](from: ElasticCall[A]) = from match {
      case TypeExists(index, typ) => true
      case ListTypes(index) => List.empty[String]
      case ListIndeces() => indices
    }
  }

  def exec(
    adir: ADir,
    indices: List[String]
  ): FileSystemError \/ List[PathSegment] =
    queryfile.listContents[ElasticCall](adir).run.foldMap(elasticInterpreter(indices)).map(_.toList)

  "ListContents" should {
    "for a /" should {
      "list all indexes if there are no multi-level folders" in {
        val indices = List("foo", "bar", "baz")
        val result = exec(rootDir, indices)

        result must be_\/-(indices.map(i => DirName(i).left[FileName]))
      }

      "list all indexes if there are multi-level folders" in {
        val indices = List(s"foo${separator}bar", "baz", s"muu${separator}nuu${separator}buu")
        val result = exec(rootDir, indices)

        result must be_\/-(List(DirName("foo"), DirName("baz"), DirName("muu")).map(_.left[FileName]))
      }
    }

    "for any folder directly under /" should {
      "return its subfolders" in {
        val indices = List(
          "folder1",
          s"folder2${separator}folder3",
          s"folder2${separator}folder4${separator}folder5",
          s"folder2${separator}folder4${separator}folder6${separator}folder7",
          s"folder8${separator}folder9${separator}folder10",
          s"folder11${separator}folder12${separator}folder13${separator}folder7"
        )
        val result = exec(rootDir </> dir("folder2"), indices)

        result must be_\/-(List(DirName("folder3"), DirName("folder4")).map(_.left[FileName]))
      }

      "return empty if no subfolders" in {
        val indices = List(
          "folder1",
          s"folder2${separator}folder3",
          s"folder2${separator}folder4${separator}folder5"
        )
        val result = exec(rootDir </> dir("folder1"), indices)

        result must be_\/-(List.empty[PathSegment])
      }
    }

    "for folder not directly under /" should {
      "return its subfolders" in {
        val indices = List(
          "folder1",
          s"folder2${separator}folder3",
          s"folder2${separator}folder4${separator}folder5",
          s"folder2${separator}folder4${separator}folder6${separator}folder7",
          s"folder8${separator}folder9${separator}folder10",
          s"folder11${separator}folder12${separator}folder13${separator}folder7"
        )
        val result = exec(rootDir </> dir("folder2") </> dir("folder4"), indices)

        result must be_\/-(List(DirName("folder5"), DirName("folder6")).map(_.left[FileName]))
      }

      "return empty if no subfolders" in {
        val indices = List(
          "folder1",
          s"folder2${separator}folder3",
          s"folder2${separator}folder4${separator}folder5"
        )
        val result = exec(rootDir </> dir("folder3"), indices)

        result must be_\/-(List.empty[PathSegment])
      }

    }
  }
}
