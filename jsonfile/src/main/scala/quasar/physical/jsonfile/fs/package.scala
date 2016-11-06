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

package quasar.physical.jsonfile

import quasar._
import quasar.Predef._
import quasar.fp._, numeric._
import quasar.fs._
import pathy.Path, Path._
import quasar.contrib.pathy._
import scalaz._, Scalaz._
import FileSystemIndependentTypes._
import matryoshka._
import RenderTree.ops._
import java.time._
import java.time.temporal.IsoFields
import monocle.Optional

package object fs {
  val FsType = FileSystemType("jsonfile")

  type ADir                = quasar.contrib.pathy.ADir
  type AFile               = quasar.contrib.pathy.AFile
  type APath               = quasar.contrib.pathy.APath
  type AsTask[F[X]]        = Task[F ~> Task]
  type EJson[A]            = quasar.ejson.EJson[A]
  type Fix[F[_]]           = matryoshka.Fix[F]
  type PathSegment         = quasar.contrib.pathy.PathSegment
  type Table               = ygg.table.TableData
  type Task[A]             = scalaz.concurrent.Task[A]
  type Type                = quasar.Type
  type MapFunc[T[_[_]], A] = quasar.qscript.MapFunc[T, A]
  type ZonedDateTime       = java.time.ZonedDateTime
  type LocalDateTime       = java.time.LocalDateTime
  type LogicalPlan[A]      = quasar.frontend.logicalplan.LogicalPlan[A]

  val Task = scalaz.concurrent.Task
  val Type = quasar.Type

  def TODO: Nothing                                  = scala.Predef.???
  def abort(msg: String): Nothing                    = throw new RuntimeException(msg)
  def cond[A](p: Boolean, ifp: => A, elsep: => A): A = if (p) ifp else elsep
  def diff[A: RenderTree](l: A, r: A): RenderedTree  = l.render diff r.render
  def showln[A: Show](x: A): Unit                    = println(x.shows)

  def partialPrism[S, A](f: PartialFunction[S, A])(g: (A, S) => S): Optional[S, A] =
    Optional[S, A](f.lift)(a => s => g(a, s))

  def instantMillis(x: Instant): Long  = x.getEpochSecond * 1000
  def nowMillis(): Long                = instantMillis(Instant.now)
  def instantFromMillis(millis: Long)  = Instant ofEpochMilli millis
  def zonedUtcFromMillis(millis: Long) = ZonedDateTime.ofInstant(instantFromMillis(millis), ZoneOffset.UTC)

  implicit class PathyRFPathOps(val path: Path[Any, Any, Sandboxed]) {
    def toAbsolute: APath = mkAbsolute(rootDir, path)
    def toJavaFile: jFile = new jFile(posixCodec unsafePrintPath path)
  }
  implicit class BooleanAlgebraOps[A](private val self: A) {
    def unary_!(implicit alg: BooleanAlgebra[A]): A     = alg.complement(self)
    def &&(that: A)(implicit alg: BooleanAlgebra[A]): A = alg.and(self, that)
    def ||(that: A)(implicit alg: BooleanAlgebra[A]): A = alg.or(self, that)
  }
  implicit class NumericAlgebraOps[A](private val self: A) {
    def unary_-(implicit alg: NumericAlgebra[A]): A     = alg.negate(self)
    def +(that: A)(implicit alg: NumericAlgebra[A]): A  = alg.plus(self, that)
    def -(that: A)(implicit alg: NumericAlgebra[A]): A  = alg.minus(self, that)
    def *(that: A)(implicit alg: NumericAlgebra[A]): A  = alg.times(self, that)
    def /(that: A)(implicit alg: NumericAlgebra[A]): A  = alg.div(self, that)
    def %(that: A)(implicit alg: NumericAlgebra[A]): A  = alg.mod(self, that)
    def **(that: A)(implicit alg: NumericAlgebra[A]): A = alg.pow(self, that)
  }
  implicit class TimeAlgebraOps[A](private val self: A)(implicit ta: TimeAlgebra[A]) {
    private implicit def toZonedDateTime(x: A): ZonedDateTime = ta asZonedDateTime x
    private implicit def toRep(x: Long): A                    = ta fromLong x

    /* SRSLY JAVA? */
    private def toZoneOffset(x: A): ZoneOffset = toZonedDateTime(x).getZone match { case x: ZoneOffset => x }

    // val ExtractCentury        = extract("Pulls out the century subfield from a date/time value (currently year/100).")
    // val ExtractDayOfMonth     = extract("Pulls out the day of month (`day`) subfield from a date/time value (1-31).")
    // val ExtractDayOfWeek      = extract("Pulls out the day of week (`dow`) subfield from a date/time value " +"(Sunday: 0 to Saturday: 7).")
    // val ExtractDayOfYear      = extract("Pulls out the day of year (`doy`) subfield from a date/time value (1-365 or -366).")
    // val ExtractDecade         = extract("Pulls out the decade subfield from a date/time value (year/10).")
    // val ExtractEpoch          = extract("Pulls out the epoch subfield from a date/time value. For dates and timestamps, this is the number of seconds since midnight, 1970-01-01. For intervals, the number of seconds in the interval.")
    // val ExtractHour           = extract("Pulls out the hour subfield from a date/time value (0-23).")
    // val ExtractIsoDayOfWeek   = extract("Pulls out the ISO day of week (`isodow`) subfield from a date/time value " +"(Monday: 1 to Sunday: 7).")
    // val ExtractIsoYear        = extract("Pulls out the ISO year (`isoyear`) subfield from a date/time value (based " +"on the first week containing Jan. 4).")
    // val ExtractMicroseconds   = extract("Pulls out the microseconds subfield from a date/time value (including seconds).")
    // val ExtractMillennium     = extract("Pulls out the millennium subfield from a date/time value (currently year/1000).")
    // val ExtractMilliseconds   = extract("Pulls out the milliseconds subfield from a date/time value (including seconds).")
    // val ExtractMinute         = extract("Pulls out the minute subfield from a date/time value (0-59).")
    // val ExtractMonth          = extract("Pulls out the month subfield from a date/time value (1-12).")
    // val ExtractQuarter        = extract("Pulls out the quarter subfield from a date/time value (1-4).")
    // val ExtractSecond         = extract("Pulls out the second subfield from a date/time value (0-59, with fractional parts).")
    // val ExtractTimezone       = extract("Pulls out the timezone subfield from a date/time value (in seconds east of UTC).")
    // val ExtractTimezoneHour   = extract("Pulls out the hour component of the timezone subfield from a date/time value.")
    // val ExtractTimezoneMinute = extract("Pulls out the minute component of the timezone subfield from a date/time value.")
    // val ExtractWeek           = extract("Pulls out the week subfield from a date/time value (1-53).")
    // val ExtractYear           = extract("Pulls out the year subfield from a date/time value.")

    def extractCentury: A        = self.getYear / 100
    def extractDayOfMonth: A     = self.getDayOfMonth
    def extractDayOfWeek: A      = self.getDayOfWeek.getValue % 7
    def extractDayOfYear: A      = self.getDayOfYear
    def extractDecade: A         = self.getYear / 10
    def extractEpoch: A          = self.toInstant.getEpochSecond
    def extractHour: A           = self.getHour
    def extractIsoDayOfWeek: A   = self.getDayOfWeek.getValue
    def extractIsoYear: A        = self.getYear
    def extractMicroseconds: A   = self.getSecond * 1000000
    def extractMillennium: A     = self.getYear / 1000
    def extractMilliseconds: A   = self.getSecond * 1000
    def extractMinute: A         = self.getMinute
    def extractMonth: A          = self.getMonth.getValue
    def extractQuarter: A        = self get IsoFields.QUARTER_OF_YEAR
    def extractSecond: A         = self.getSecond
    def extractTimezone: A       = toZoneOffset(self).getTotalSeconds
    def extractTimezoneHour: A   = toZoneOffset(self).getTotalSeconds / (60 * 60)
    def extractTimezoneMinute: A = toZoneOffset(self).getTotalSeconds / 60
    def extractWeek: A           = self get IsoFields.WEEK_OF_WEEK_BASED_YEAR
    def extractYear: A           = self.getYear
  }

  implicit def showPath: Show[APath]      = Show shows (posixCodec printPath _)
  implicit def showRHandle: Show[RHandle] = Show shows (r => "ReadHandle(%s, %s)".format(r.file.show, r.id))
  implicit def showWHandle: Show[WHandle] = Show shows (r => "WriteHandle(%s, %s)".format(r.file.show, r.id))
  implicit def showFixPlan: Show[FixPlan] = Show shows (lp => FPlan("", lp).toString)
}
