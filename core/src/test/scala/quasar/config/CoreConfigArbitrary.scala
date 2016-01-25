package quasar.config

import quasar.Predef._
import quasar.{Variables, VarName, VarValue}
import quasar.fs.{Path => QPath}
import quasar.sql

import com.mongodb.ConnectionString
import org.scalacheck.{Arbitrary, Gen}

trait CoreConfigArbitrary {
  import Arbitrary._, Gen._

  implicit val coreConfigArbitrary: Arbitrary[CoreConfig] = Arbitrary {
    for {
      mounts <- listOf(mountGen)
    } yield CoreConfig(Map(mounts: _*))
  }

  private def mountGen: Gen[(QPath, MountConfig)] = for {
    path <- arbitrary[String]
    cfg  <- Gen.oneOf(mongoCfgGen, viewCfgGen)
  } yield (QPath(path), cfg)

  private def mongoCfgGen =
    Gen.const(MongoDbConfig(new ConnectionString("mongodb://localhost/test")))

  private val SimpleQuery = sql.Select(sql.SelectAll,
    List(sql.Proj(sql.Splice(None), None)),
    Some(sql.TableRelationAST("foo", None)),
    None, None, None)

  private def viewCfgGen = for {
    vars <- listOf(for {
      n <- alphaChar
      x <- choose(0, 100)
    } yield VarName(n.toString) -> VarValue(x.toString))
  } yield ViewConfig(SimpleQuery, Variables(vars.toMap))
}

object CoreConfigArbitrary extends CoreConfigArbitrary
