/*
 *  ____    ____    _____    ____    ___     ____
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either version
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog.mimir

import com.precog.common._
import com.precog.common.Path
import com.precog.common.accounts._

import com.precog.yggdrasil._
import com.precog.yggdrasil.execution.EvaluationContext
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.vfs._

import org.specs2.mutable.Specification

import blueeyes._, json._
import scalaz._, Scalaz._

trait FSLibSpecs extends Specification with FSLibModule[Need] with TestColumnarTableModule[Need] {
  implicit def M = Need.need

  import trans._
  import constants._

  val library = new FSLib {}
  import library._

  lazy val projectionMetadata: Map[Path, Map[ColumnRef, Long]] = Map(
    Path("/foo/bar1/baz/quux1")   -> Map(ColumnRef(CPath.Identity, CString) -> 10L),
    Path("/foo/bar2/baz/quux1")   -> Map(ColumnRef(CPath.Identity, CString) -> 20L),
    Path("/foo/bar2/baz/quux2")   -> Map(ColumnRef(CPath.Identity, CString) -> 30L),
    Path("/foo2/bar1/baz/quux1" ) -> Map(ColumnRef(CPath.Identity, CString) -> 40L)
  )

  val vfs = new StubVFSMetadata[Need](projectionMetadata)

  def pathTable(path: String) = {
    Table.constString(Set(path)).transform(WrapObject(Leaf(Source), TransSpecModule.paths.Value.name))
  }

  val testAPIKey = "testAPIKey"
  def testAccount = AccountDetails("00001", "test@email.com", dateTime.now, "testAPIKey", Path.Root, AccountPlan.Free)
  val defaultEvaluationContext = EvaluationContext(testAPIKey, testAccount, Path.Root, Path.Root, dateTime.now)
  val defaultMorphContext = MorphContext(defaultEvaluationContext, new MorphLogger {
    def info(msg: String): Need[Unit]  = M.point(())
    def warn(msg: String): Need[Unit]  = M.point(())
    def error(msg: String): Need[Unit] = M.point(())
    def die(): Need[Unit]              = M.point(sys.error("MorphContext#die()"))
  })

  def runExpansion(table: Table): List[JValue] = {
    expandGlob(table, defaultMorphContext).map(_.transform(SourceValue.Single)).flatMap(_.toJson).copoint.toList
  }

  "path globbing" should {
    "not alter un-globbed paths" in {
      val table = pathTable("/foo/bar/baz/")
      val expected: List[JValue] = List(JString("/foo/bar/baz/"))
      runExpansion(table) must_== expected
    }

    "not alter un-globbed relative paths" in {
      val table = pathTable("foo")
      val expected: List[JValue] = List(JString("/foo/"))
      runExpansion(table) mustEqual expected
    }

    "expand a leading glob" in {
      val table = pathTable("/*/bar1")
      val expected: List[JValue] = List(JString("/foo/bar1/"), JString("/foo2/bar1/"))
      runExpansion(table) must_== expected
    }

    "expand a trailing glob" in {
      val table = pathTable("/foo/*")
      val expected: List[JValue] = List(JString("/foo/bar1/"), JString("/foo/bar2/"))
      runExpansion(table) must_== expected
    }

    "expand an internal glob and filter" in {
      val table = pathTable("/foo/*/baz/quux1")
      val expected: List[JValue] = List(JString("/foo/bar1/baz/quux1/"), JString("/foo/bar2/baz/quux1/"))
      runExpansion(table) must_== expected
    }

    "expand multiple globbed segments" in {
      val table = pathTable("/foo/*/baz/*")
      val expected: List[JValue] = List(JString("/foo/bar1/baz/quux1/"), JString("/foo/bar2/baz/quux1/"), JString("/foo/bar2/baz/quux2/"))
      runExpansion(table) must_== expected
    }
  }
}

object FSLibSpecs extends FSLibSpecs
