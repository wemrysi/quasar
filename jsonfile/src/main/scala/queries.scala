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

package quasar.physical.jsonfile.fs

import ygg._, common._
import matryoshka._, Recursive.ops._
import quasar._, sql._, SemanticAnalysis._, RenderTree.ops._
import quasar.fp.ski._
import pathy.Path._
import quasar.contrib.pathy._
import scalaz._, Scalaz._

final case class FPlan(sql: String, lp: Fix[LogicalPlan]) {
  def universe                      = lp.universe
  def toTransSpec: Fix[LogicalPlan] = lp.cata[Fix[LogicalPlan]](x => Fix(x))

  override def toString = sql + "\n" + indent(2, lp.render.shows) + "\n"
}

object FPlan {
  def compile(query: String): String \/ Fix[LogicalPlan] =
    for {
      select <- fixParser.parse(Query(query)).leftMap(_.toString)
      attr   <- AllPhases(select).leftMap(_.toString)
      cld    <- Compiler.compile(attr).leftMap(_.toString)
    } yield cld

  def fromString(q: String): FPlan = (
    compile(q) map Optimizer.optimize flatMap (q =>
      (LogicalPlan ensureCorrectTypes q).disjunction
        leftMap (_.list.toList mkString ";")
    )
  ).fold(abort, FPlan(q, _))

  def fromFile(file: jFile): FPlan = FPlan(
    file.slurpString,
    LogicalPlan.Read(
      sandboxCurrent(
        posixCodec.parsePath(Some(_), Some(_), κ(None), κ(None))(file.getPath).get
      ).get
    )
  )
}

object Queries {
  def zipsFile = new jFile("it/src/main/resources/tests/zips.data")
  def zips     = ygg.table.ColumnarTable(zipsFile)

  def fplans: Vector[FPlan] = Vector(
    """SELECT * FROM zips OFFSET 100 LIMIT 5""",
    """SELECT COUNT(*) as cnt, LENGTH(city) FROM zips""",
    """SELECT DISTINCT SUM(pop) AS totalPop, city, state FROM zips GROUP BY city ORDER BY totalPop DESC""",
    """SELECT DISTINCT SUM(pop) AS totalPop, city, state FROM zips GROUP BY city""",
    """SELECT _id as zip, loc as loc, loc[*] as coord FROM zips""",
    """SELECT city FROM zips LIMIT 5""",
    """SELECT city, COUNT(*) AS cnt FROM zips ORDER BY cnt DESC""",
    """SELECT city, SUM(pop) AS pop FROM zips GROUP BY city ORDER BY pop""",
    """SELECT city, pop FROM zips ORDER BY pop DESC LIMIT 5""",
    """select "" || city || "" from zips""",
    """select * from zips group by city""",
    """select * from zips limit 5 offset 0""",
    """select * from zips order by pop""",
    """select * from zips order by pop, city desc""",
    """select * from zips order by pop, state, city, a4, a5, a6""",
    """select * from zips order by pop/10 desc""",
    """select * from zips where 43.058514 in loc[_]""",
    """select * from zips where city !~ "^B[AEIOU]+LD.*"""",
    """select * from zips where city !~* "^B[AEIOU]+LD.*"""",
    """select * from zips where city <> state and pop < 10000""",
    """select * from zips where city <> state""",
    """select * from zips where city is not null""",
    """select * from zips where city ~ "^B[AEIOU]+LD.*"""",
    """select * from zips where city ~* "^B[AEIOU]+LD.*"""",
    """select * from zips where length(city) < 4 and pop < 20000""",
    """select * from zips where length(city) < 4""",
    """select * from zips where not (pop = 0)""",
    """select * from zips where not (pop > 0 and pop < 1000)""",
    """select * from zips where pop in loc[_]""",
    """select * from zips where state in "PA"""",
    """select * from zips where state in ("AZ", "CO")""",
    """select * from zips where state in ("NV")""",
    """select * from zips where true""",
    """select *, "1", "2" from zips""",
    """select *, city as city2, pop as pop2 from zips""",
    """select *, pop from zips""",
    """select _id as zip, loc[*] from zips order by loc[*]""",
    """select avg(pop), min(city) from zips where state = "CO"""",
    """select case when pop < 10000 then city else loc end from zips""",
    """select case when pop > 1000 then city else lower(city) end, count(*) from zips group by city""",
    """select city from zips group by city having count(*) > 10""",
    """select city from zips group by city""",
    """select city from zips group by lower(city)""",
    """select city || ", " || state from zips""",
    """select city || ", " || state, sum(pop) from zips group by city, state""",
    """select city, count(city) from zips group by city""",
    """select city, length(city) from zips""",
    """select city, loc[0] from zips""",
    """select city, pop from zips where pop <= 1000 order by pop desc, city""",
    """select city, pop from zips where pop > 1000 order by length(city)""",
    """select city, state, sum(pop) from zips""",
    """select city, sum(pop) from zips group by city having sum(pop) > 50000""",
    """select city, sum(pop) from zips group by lower(city)""",
    """select city, sum(pop-1)/1000 from zips group by city""",
    """select count(*) as cnt, city from zips group by city""",
    """select count(distinct *) from zips""",
    """select count(distinct substring(city, 0, 1)) from zips""",
    """select count(distinct(city)) from zips""",
    """select distinct * from zips""",
    """select distinct city from zips order by city""",
    """select distinct city from zips order by pop desc""",
    """select distinct city, state from zips""",
    """select foo is null from zips where foo is null""",
    """select length(city) + 1 from zips""",
    """select length(city) as len, count(*) as cnt from zips group by length(city)""",
    """select length(city) from zips""",
    """select loc from zips where loc[0] < -73""",
    """select loc || [ 0, 1, 2 ] from zips""",
    """select loc || [ pop ] from zips where city = "BOULDER"""",
    """select loc[*] from zips where loc[*] < 0""",
    """select loc[*] from zips""",
    """select loc[0] from zips""",
    """select max(pop)/1000, pop from zips""",
    """select pop, sum(pop), pop/1000 from zips""",
    """select pop/1000 as popInK from zips order by popInK""",
    """select pop/1000 as popInK from zips where pop >= 1000 order by popInK""",
    """select state as state2, *, city as city2, *, pop as pop2 from zips where pop < 1000""",
    """select state, count(distinct(city)) from zips group by state""",
    """select state, length(min(city)) as shortest from zips group by state""",
    """select sum(avg(pop)), min(city) from zips group by foo""",
    """select sum(pop) * 100 from zips""",
    """select sum(pop) as sm from zips where state="CO" group by city""",
    """select z1.city as city1, z1.loc, z2.city as city2, z2.pop from zips as z1 join zips as z2 on z1.loc[*] = z2.loc[*]""",
    """select zips2.city from zips join zips2 on zips._id < zips2._id""",
    """select zips2.city from zips join zips2 on zips._id = zips2._id""",
    """select zips2.city from zips, zips2 where zips.pop < zips2.pop""",
    ""
  ).flatMap(s => Try(FPlan fromString s).toOption.toList)
}
