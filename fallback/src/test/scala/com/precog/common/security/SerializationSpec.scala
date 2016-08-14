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
package com.precog.common
package security

import ingest._
import service.v1
import blueeyes._, json._, serialization._, DefaultSerialization._
import scalaz._, Scalaz._, Validation.FlatMap._
// import quasar.precog.TestSupport._

class SerializationSpec extends quasar.QuasarSpecification {
  import Permission._

  val i0 = instant.zero

  "APIKeyRecord deserialization" should {
    "Handle V1 formats" in {
      val inputs = """[
        {"isRoot" : true, "name" : "root-apiKey", "description" : "The root API key", "apiKey" : "17D42117-EF8E-4F43-B833-005F4EBB262C", "grants" : ["6f89110c953940cbbccc397f68c4cc9293af764c4d034719bf35b4736ee702daaef154314d5441ba8a69ed65e4ffa581"] },
        {"isRoot" : true, "name" : "root-apiKey", "description" : "The root API key", "apiKey" : "01D60F6D-E8B6-480C-8D55-2986853D67A6", "grants" : ["e5fa39314ca748818e52c50d2d445a6f4d9f9a224ddb4e55bf7c03e2a21fb36ff2bbff861aec43a18cccf2ee7f38841e"] },
        {"isRoot" : false, "issuerKey" : "17D42117-EF8E-4F43-B833-005F4EBB262C",      "apiKey" : "F2440B9B-D8CA-42AD-BF83-C693F0A5F018", "grants" : ["75826da768b64748b8423cdd047d7e8f6361e5bb50d8428080feaf1c0c6269600982be9e1c9f4299bf521aac95065ace"] },
        {"isRoot" : true, "name" : "root-apiKey", "description" : "The root API key", "apiKey" : "D9302C66-1F43-412E-B277-4E2EF675A304", "grants" : ["f0fcc670d19d44fd9f99bd5d03e569fbcab6b5a679fb48089944772d16a43eb73643dca2c885431db100fb3d650c342b"] },
        {"isRoot" : true, "name" : "root-apiKey", "description" : "The root API key", "apiKey" : "A09D8293-A28F-4422-B375-9C0CDF75DC68", "grants" : ["c6ab82c1f69640de9e5211ebb2b96661e1bff7d8a4134f25ad1aaf1319fa7b3e182e6aa8eb1f4699b1303f0d03022213"] }
      ]"""

      val records = for {
        jv <- JParser.parseFromString(inputs)
        records <- jv.validated[List[APIKeyRecord]]
      } yield records


      records mustEqual Success(List(
        APIKeyRecord("17D42117-EF8E-4F43-B833-005F4EBB262C", Some("root-apiKey"), Some("The root API key"), "(undefined)", Set("6f89110c953940cbbccc397f68c4cc9293af764c4d034719bf35b4736ee702daaef154314d5441ba8a69ed65e4ffa581"), true),
        APIKeyRecord("01D60F6D-E8B6-480C-8D55-2986853D67A6", Some("root-apiKey"), Some("The root API key"), "(undefined)", Set("e5fa39314ca748818e52c50d2d445a6f4d9f9a224ddb4e55bf7c03e2a21fb36ff2bbff861aec43a18cccf2ee7f38841e"), true),
        APIKeyRecord("F2440B9B-D8CA-42AD-BF83-C693F0A5F018", None, None, "17D42117-EF8E-4F43-B833-005F4EBB262C",  Set("75826da768b64748b8423cdd047d7e8f6361e5bb50d8428080feaf1c0c6269600982be9e1c9f4299bf521aac95065ace"), false),
        APIKeyRecord("D9302C66-1F43-412E-B277-4E2EF675A304", Some("root-apiKey"), Some("The root API key"), "(undefined)", Set("f0fcc670d19d44fd9f99bd5d03e569fbcab6b5a679fb48089944772d16a43eb73643dca2c885431db100fb3d650c342b"), true),
        APIKeyRecord("A09D8293-A28F-4422-B375-9C0CDF75DC68", Some("root-apiKey"), Some("The root API key"), "(undefined)", Set("c6ab82c1f69640de9e5211ebb2b96661e1bff7d8a4134f25ad1aaf1319fa7b3e182e6aa8eb1f4699b1303f0d03022213"), true)
      ))
    }
  }

  "Grant deserialization" should {
    "Handle V1 formats" in {
      val inputs = """[
        {
          "name" : "root-grant", "description" : "The root grant",
          "permissions" : [
            {"accessType" : "read", "path" : "/" },
            {"accessType" : "reduce", 	"path" : "/" },
            {"accessType" : "write", 	"path" : "/" },
            {"accessType" : "delete", 	"path" : "/" }
          ],
          "parentIds" : [ ],
          "grantId" : "6f89110c953940cbbccc397f68c4cc9293af764c4d034719bf35b4736ee702daaef154314d5441ba8a69ed65e4ffa581"
        },
        {
          "name" : "root-grant", "description" : "The root grant",
          "permissions" : [
            {"accessType" : "read", 	"path" : "/" },
            {"accessType" : "reduce", 	"path" : "/" },
            {"accessType" : "write", 	"path" : "/" },
            {"accessType" : "delete", 	"path" : "/" }
          ],
          "parentIds" : [ ],
          "grantId" : "e5fa39314ca748818e52c50d2d445a6f4d9f9a224ddb4e55bf7c03e2a21fb36ff2bbff861aec43a18cccf2ee7f38841e"
        },
        {
          "permissions" : [
            {"accessType" : "read", 	"path" : "/", "ownerAccountIds" : [	"0000000001" ] },
            {"accessType" : "reduce", 	"path" : "/", "ownerAccountIds" : [ 	"0000000001" ] },
            {"accessType" : "write", 	"path" : "/0000000001/" },
            {"accessType" : "delete", 	"path" : "/0000000001/" }
          ],
          "parentIds" : [ "6f89110c953940cbbccc397f68c4cc9293af764c4d034719bf35b4736ee702daaef154314d5441ba8a69ed65e4ffa581" ],
          "issuerKey" : "17D42117-EF8E-4F43-B833-005F4EBB262C",
          "grantId" : "75826da768b64748b8423cdd047d7e8f6361e5bb50d8428080feaf1c0c6269600982be9e1c9f4299bf521aac95065ace"
        }
      ]"""

      (for {
        jv <- JParser.parseFromString(inputs)
        records <- jv.validated[List[Grant]]
      } yield {
        records mustEqual List(
          Grant(
            "6f89110c953940cbbccc397f68c4cc9293af764c4d034719bf35b4736ee702daaef154314d5441ba8a69ed65e4ffa581",
            Some("root-grant"), Some("The root grant"), "(undefined)", Set(),
            Set(
              ReadPermission(Path("/"), WrittenByAny),
              ReducePermission(Path("/"), WrittenByAny),
              WritePermission(Path("/"), WriteAsAny),
              DeletePermission(Path("/"), WrittenByAny)
            ), i0, None
          ),
          Grant(
            "e5fa39314ca748818e52c50d2d445a6f4d9f9a224ddb4e55bf7c03e2a21fb36ff2bbff861aec43a18cccf2ee7f38841e",
            Some("root-grant"), Some("The root grant"), "(undefined)", Set(),
            Set(
              ReadPermission(Path("/"), WrittenByAny),
              ReducePermission(Path("/"), WrittenByAny),
              WritePermission(Path("/"), WriteAsAny),
              DeletePermission(Path("/"), WrittenByAny)
            ), i0, None
          ),
          Grant(
            "75826da768b64748b8423cdd047d7e8f6361e5bb50d8428080feaf1c0c6269600982be9e1c9f4299bf521aac95065ace",
            None, None, "17D42117-EF8E-4F43-B833-005F4EBB262C",
            Set("6f89110c953940cbbccc397f68c4cc9293af764c4d034719bf35b4736ee702daaef154314d5441ba8a69ed65e4ffa581"),
            Set(
              ReadPermission(Path("/"), WrittenByAccount("0000000001")),
              ReducePermission(Path("/"), WrittenByAccount("0000000001")),
              WritePermission(Path("/0000000001/"), WriteAsAny),
              DeletePermission(Path("/0000000001/"), WrittenByAny)), i0, None)
        )
      }).fold({ error => throw new Exception(error.toString) }, _ => ok)
    }

    "Deserialize NewGrantRequest without parentIds" in {
      (JObject("permissions" -> JArray())).validated[v1.NewGrantRequest] must beLike {
        case Success(_) => ok
      }
    }
  }

  "Ingest serialization" should {
    "Handle V1 format" in {
      (JObject("apiKey" -> JString("1234"),
               "path"    -> JString("/test/"),
               "data"    -> JObject("test" -> JNum(1)),
               "metadata" -> JArray())).validated[Ingest] must beLike {
        case Success(_) => ok
      }
    }
  }

  "Archive serialization" should {
    "Handle V1 format" in {
      JObject("apiKey" -> JString("1234"),
              "path"   -> JString("/test/")).validated[Archive] must beLike {
        case Success(_) => ok
      }
    }
  }
}
