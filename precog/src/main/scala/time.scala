package ygg.pkg

import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter.ISO_DATE_TIME

trait PackageTime {
  type Instant  = java.time.Instant
  type Period   = java.time.Period
  type Duration = java.time.Duration
  type DateTime = java.time.LocalDateTime

  implicit class QuasarDurationOps(private val x: Duration) {
    def getMillis: Long = x.toMillis
  }
  implicit class QuasarInstantOps(private val x: Instant) {
    def getMillis: Long         = x.toEpochMilli
    def -(y: Instant): Duration = java.time.Duration.between(x, y)
  }
  implicit class QuasarPeriodOps(private val x: Period) {
    def getMillis: Long      = toDuration.getMillis
    def toDuration: Duration = java.time.Duration from x
  }
  implicit class QuasarDateTimeOps(private val x: DateTime) {
    def until(end: DateTime): Period = java.time.Period.between(x.toLocalDate, end.toLocalDate)
    def toUtcInstant: Instant        = x toInstant UTC
    def getMillis: Long              = toUtcInstant.toEpochMilli
  }

  object duration {
    def fromMillis(ms: Long): Duration = java.time.Duration ofMillis ms
  }
  object period {
    def fromMillis(ms: Long): Period = java.time.Period from (duration fromMillis ms)
  }
  object instant {
    def zero: Instant                             = fromMillis(0L)
    def ofEpoch(secs: Long, nanos: Long): Instant = java.time.Instant.ofEpochSecond(secs, nanos)
    def now(): Instant                            = java.time.Instant.now
    def fromMillis(ms: Long): Instant             = java.time.Instant.ofEpochMilli(ms)
    def apply(s: String): Instant                 = java.time.Instant parse s
  }
  object dateTime {
    def minimum: DateTime              = java.time.LocalDateTime.MIN
    def maximum: DateTime              = java.time.LocalDateTime.MAX
    def fromIso(s: String): DateTime   = java.time.LocalDateTime.parse(s, ISO_DATE_TIME)
    def showIso(d: DateTime): String   = d format ISO_DATE_TIME
    def zero: DateTime                 = fromMillis(0)
    def now(): DateTime                = java.time.LocalDateTime.now
    def fromMillis(ms: Long): DateTime = java.time.LocalDateTime.ofInstant(instant fromMillis ms, UTC)
    def apply(s: String): DateTime     = java.time.LocalDateTime.parse(s)
  }
}
