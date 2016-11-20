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

import ygg._, common._
import quasar._
import quasar.fs._
import scalaz._, Scalaz._
import FileSystemIndependentTypes._
import java.time._
import java.time.temporal.IsoFields

package object fs {
  val FsType = FileSystemType("jsonfile")

  def TODO: Nothing                                  = scala.Predef.???
  def abort(msg: String): Nothing                    = throw new RuntimeException(msg)
  def cond[A](p: Boolean, ifp: => A, elsep: => A): A = if (p) ifp else elsep
  def diff[A: RenderTree](l: A, r: A): RenderedTree  = l.render diff r.render
  def showln[A: Show](x: A): Unit                    = println(x.shows)

  def True[A](implicit z: BooleanAlgebra[A]): A  = z fromBool true
  def False[A](implicit z: BooleanAlgebra[A]): A = z fromBool false

  def partialPrism[S, A](f: PartialFunction[S, A])(g: (A, S) => S): OptLens[S, A] =
    OptLens[S, A](f.lift)(a => s => g(a, s))

  def instantMillis(x: Instant): Long  = x.getEpochSecond * 1000
  def nowMillis(): Long                = instantMillis(Instant.now)
  def instantFromMillis(millis: Long)  = Instant ofEpochMilli millis
  def zonedUtcFromMillis(millis: Long) = ZonedDateTime.ofInstant(instantFromMillis(millis), ZoneOffset.UTC)

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
}
