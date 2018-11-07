[![Travis](https://travis-ci.org/slamdata/quasar.svg?branch=master)](https://travis-ci.org/slamdata/quasar)
[![AppVeyor](https://ci.appveyor.com/api/projects/status/pr5he90wye6ii8ml/branch/master?svg=true)](https://ci.appveyor.com/project/jdegoes/quasar/branch/master)
[![Discord](https://img.shields.io/discord/373302030460125185.svg?logo=discord)](https://discord.gg/QNjwCg6)

# Quasar

Quasar is an open source NoSQL analytics engine that can be used as a library or through a REST API to power advanced analytics across a growing range of data sources and databases, including MongoDB.

## SQL²

SQL² is the dialect of SQL that Quasar understands.

In the following documentation SQL² will be used interchangeably with SQL.

SQL² supports variables inside queries (`SELECT * WHERE pop < :cutoff`). Values for these variables, which can be any expression, should be specified as additional parameters in the url, using the variable name prefixed by `var.` (e.g. `var.cutoff=1000`). Failure to specify valid values for all variables used inside a query will result in an error. These values use the same syntax as the query itself; notably, strings should be surrounded by double quotes. Some acceptable values are `123`, `"CO"`, and `DATE("2015-07-06")`.

## Building from Source

**Note**: This requires Java 8 and Bash (Linux, Mac).

### Build

The following sections explain how to build and run the various subprojects.

#### Basic Compile & Test

To compile the project and run tests, first clone the quasar repo and then execute the following command (if on Windows, reverse the slashes):

```bash
./sbt test
```

Note: please note that we are not using here a system wide sbt, but our own copy of it (under ./sbt). This is primarily done for determinism. In order to have a reproducible build, the helper script needs to be part of the repo.

#### REPL JAR

To build a JAR for the REPL, which allows entering commands at a command-line prompt, execute the following command:

```bash
./sbt 'repl/assembly'
```

The path of the JAR will be `repl/target/scala-2.12/quasar-repl-assembly-[version].jar`, where `[version]` is the Quasar version number.

To run the JAR, execute the following command:

```bash
java -jar [<path to jar>] [-c <config file>]
```

### Backends

By default, the REPL assembly contains only the local datasource. In order to use other datasources you will need to add them. See the repl `help` command for more information.

## REPL Usage

The interactive REPL accepts SQL `SELECT` queries.

```
💪 $ select * from zips where state="CO" limit 3

Query time: 0.1s
 city    | loc[0]       | loc[1]     | pop    | state |
---------|--------------|------------|--------|-------|
 ARVADA  |  -105.098402 |  39.794533 |  12065 | CO    |
 ARVADA  |  -105.065549 |  39.828572 |  32980 | CO    |
 ARVADA  |   -105.11771 |  39.814066 |  33260 | CO    |

💪 $ select city from zips limit 3
...
 city     |
----------|
 AGAWAM   |
 CUSHMAN  |
 BARRE    |
```

You may also store the result of a SQL query:

```sql
💪 $ out1 := select * from zips where state="CO" limit 3
```

The location of a collection may be specified as an absolute path by
surrounding the path with double quotes:

```sql
select * from `/test/zips`
```

Type `help` for information on other commands.

## Schema

Given query results like:

```json
{"_id":"01001","city":"AGAWAM","loc":[-72.622739,42.070206],"pop":15338,"state":"MA"}
{"_id":"01002","city":"CUSHMAN","loc":[-72.51565,42.377017],"pop":36963,"state":"MA"}
{"_id":"01005","city":"BARRE","loc":[-72.108354,42.409698],"pop":4546,"state":"MA"}
{"_id":"01007","city":"BELCHERTOWN","loc":[-72.410953,42.275103],"pop":10579,"state":"MA"}
{"_id":"01008","city":"BLANDFORD","loc":[-72.936114,42.182949],"pop":1240,"state":"MA"}
{"_id":"01010","city":"BRIMFIELD","loc":[-72.188455,42.116543],"pop":3706,"state":"MA"}
{"_id":"01011","city":"CHESTER","loc":[-72.988761,42.279421],"pop":1688,"state":"MA"}
{"_id":"01012","city":"CHESTERFIELD","loc":[-72.833309,42.38167],"pop":177,"state":"MA"}
{"_id":"01013","city":"CHICOPEE","loc":[-72.607962,42.162046],"pop":23396,"state":"MA"}
{"_id":"01020","city":"CHICOPEE","loc":[-72.576142,42.176443],"pop":31495,"state":"MA"}
```

a schema document might look like

```json
{
  "measure" : {
    "kind" : "collection",
    "count" : 1000.0,
    "minLength" : 5.0,
    "maxLength" : 5.0
  },
  "structure" : {
    "type" : "map",
    "of" : {
      "city" : {
        "measure" : {
          "kind" : "collection",
          "count" : 1000.0,
          "minLength" : 3.0,
          "maxLength" : 16.0
        },
        "structure" : {
          "tag" : "_structural.string",
          "type" : "array",
          "of" : {
            "measure" : {
              "count" : 8693.0,
              "distribution" : {
                "state" : {
                  "centralMoment4" : 893992600.3364398,
                  "size" : 8693.0,
                  "centralMoment3" : -18773123.74002289,
                  "centralMoment2" : 876954.1582882765,
                  "centralMoment1" : 74.29506499482345
                },
                "variance" : 100.89210288636407,
                "kurtosis" : 10.111128909991152,
                "mean" : 74.29506499482345,
                "skewness" : -2.1317240928957726
              },
              "min" : " ",
              "max" : "Z",
              "kind" : "char"
            },
            "structure" : {
              "type" : "character"
            }
          }
        }
      },
      "state" : {
        "measure" : {
          "kind" : "collection",
          "count" : 1000.0,
          "minLength" : 2.0,
          "maxLength" : 2.0
        },
        "structure" : {
          "tag" : "_structural.string",
          "type" : "array",
          "of" : {
            "measure" : {
              "count" : 2000.0,
              "distribution" : {
                "state" : {
                  "centralMoment4" : 11285757.38865178,
                  "size" : 2000.0,
                  "centralMoment3" : 11979.395483999382,
                  "centralMoment2" : 103139.03800000004,
                  "centralMoment1" : 76.19100000000014
                },
                "variance" : 51.59531665832919,
                "kurtosis" : 2.1271635715970967,
                "mean" : 76.19100000000014,
                "skewness" : 0.01618606190840656
              },
              "min" : "A",
              "max" : "Z",
              "kind" : "char"
            },
            "structure" : {
              "type" : "character"
            }
          }
        }
      },
      "pop" : {
        "measure" : {
          "count" : 1000.0,
          "distribution" : {
            "state" : {
              "centralMoment4" : 2.323080620322664E+20,
              "size" : 1000.0,
              "centralMoment3" : 4322812032420233.5,
              "centralMoment2" : 150795281801.8999,
              "centralMoment1" : 8721.71000000001
            },
            "variance" : 150946228.02992985,
            "kurtosis" : 10.267451061747597,
            "mean" : 8721.71000000001,
            "skewness" : 2.337959182172043
          },
          "min" : 0,
          "max" : 94317,
          "kind" : "decimal"
        },
        "structure" : {
          "type" : "decimal"
        }
      },
      "_id" : {
        "measure" : {
          "kind" : "collection",
          "count" : 1000.0,
          "minLength" : 5.0,
          "maxLength" : 5.0
        },
        "structure" : {
          "tag" : "_structural.string",
          "type" : "array",
          "of" : {
            "measure" : {
              "count" : 5000.0,
              "distribution" : {
                "state" : {
                  "centralMoment4" : 556673.1571508175,
                  "size" : 5000.0,
                  "centralMoment3" : 7962.505025040006,
                  "centralMoment2" : 38822.78220000003,
                  "centralMoment1" : 52.24340000000003
                },
                "variance" : 7.766109661932393,
                "kurtosis" : 1.8485506875431554,
                "mean" : 52.24340000000003,
                "skewness" : 0.07362665279751003
              },
              "min" : "0",
              "max" : "9",
              "kind" : "char"
            },
            "structure" : {
              "type" : "character"
            }
          }
        }
      },
      "loc" : {
        "measure" : {
          "kind" : "collection",
          "count" : 1000.0,
          "minLength" : 2.0,
          "maxLength" : 2.0
        },
        "structure" : {
          "type" : "array",
          "of" : [
            {
              "measure" : {
                "count" : 1000.0,
                "distribution" : {
                  "state" : {
                    "centralMoment4" : 281712013.3937695,
                    "size" : 1000.0,
                    "centralMoment3" : -4328243.124174622,
                    "centralMoment2" : 212160.04270665144,
                    "centralMoment1" : -90.52571468599996
                  },
                  "variance" : 212.3724151217732,
                  "kurtosis" : 6.290020275246902,
                  "mean" : -90.52571468599996,
                  "skewness" : -1.4027118290018674
                },
                "min" : -170.293408,
                "max" : -67.396382,
                "kind" : "decimal"
              },
              "structure" : {
                "type" : "decimal"
              }
            },
            {
              "measure" : {
                "count" : 1000.0,
                "distribution" : {
                  "state" : {
                    "centralMoment4" : 3748702.702983835,
                    "size" : 1000.0,
                    "centralMoment3" : 15799.696678343358,
                    "centralMoment2" : 26853.295673558245,
                    "centralMoment1" : 39.09175202499995
                  },
                  "variance" : 26.880175849407653,
                  "kurtosis" : 5.224679782347093,
                  "mean" : 39.09175202499995,
                  "skewness" : 0.1137115453464455
                },
                "min" : 20.907097,
                "max" : 65.824542,
                "kind" : "decimal"
              },
              "structure" : {
                "type" : "decimal"
              }
            }
          ]
        }
      }
    }
  }
}
```

Schema documents represent an estimate of the structure of the given dataset and are generated from a random sample of the data. Each node of the resulting structure is annotated with the frequency the node was observed and the bounds of the observed values, when available (NB: bounds should be seen as a reference and not taken as the true, global maximum or minimum values). Additionally, for numeric values, statistical distribution information is included.

When two documents differ in structure, their differences are accumulated in a union. Basic frequency information is available for the union and more specific annotations are preserved as much as possible for the various members.

The `arrayMaxLength`, `mapMaxSize`, `stringMaxLength` and `unionMaxSize` parameters allow for control over the amount of information contained in the returned schema by limiting the size of various structures in the result. Structures that exceed the various size thresholds are compressed using various heuristics depending on the structure involved.

## Data Formats

Quasar produces and accepts data in two JSON-based formats or CSV (`text/csv`). Each JSON-based format can
represent all the types of data that Quasar supports. The two formats are appropriate for
different purposes.

Json can either be line delimited (`application/ldjson`/`application/x-ldjson`) or a single json value (`application/json`).

### Precise JSON

This format is unambiguous, allowing every value of every type to be specified. It's useful for
entering data, and for extracting data to be read by software (as opposed to people.) Contains
extra information that can make it harder to read.


### Readable JSON

This format is easy to read and use with other tools, and contains minimal extra information.
It does not always convey the precise type of the source data, and does not allow all values
to be specified. For example, it's not possible to tell the difference between the string
`"12:34:56"` and the time value equal to 34 minutes and 56 seconds after noon.


### Examples

Type      | Readable        | Precise  | Notes
----------|-----------------|----------|------
null      | `null`          | *same*   |
boolean   | `true`, `false` | *same*   |
string    | `"abc"`         | *same*   |
number    | `1`, `2.1`      | *same*   |
object    | `{ "a": 1 }`    | *same*   | Keys that coincidentally equal a precise temporal key (e.g. "$localtime") are not supported.
array     | `[1, 2, 3]`     | *same*   |
localdatetime  | `"2015-01-31T10:30:00"`  | `{ "$localdatetime": "2015-01-31T10:30" }`   |
localdate      | `"2015-01-31"`           | `{ "$localdate": "2015-01-31" }`             |
localtime      | `"10:30:00.000"`         | `{ "$localtime": "10:30" }`                  |
offsetdatetime | `"2015-01-31T10:30:00Z"` | `{ "$offsetdatetime": "2015-01-31T10:30Z" }` |
offsetdate     | `"2015-01-31Z"`          | `{ "$offsetdate": "2015-01-31Z" }`           |
offsettime     | `"10:30:00.000Z"`        | `{ "$offsettime": "10:30Z" }`                |
interval       | `"PT12H34M"`             | `{ "$interval": "P7DT12H34M" }`              |


### CSV

When Quasar produces CSV, all fields and array elements are "flattened" so that each column in the output contains the data for a single location in the source document. For example, the document `{ "foo": { "bar": 1, "baz": 2 } }` becomes

```
foo.bar,foo.baz
1,2
```

Data is formatted the same way as the "Readable" JSON format, except that all values including `null`, `true`, `false`, and numbers are indistinguishable from their string representations.

It is possible to use the `columnDelimiter`, `rowDelimiter` `quoteChar` and `escapeChar` media-type extensions keys in order to customize the layout of the csv. If some or all of these extensions are not specified, they will default to the following values:

- columnDelimiter: `,`
- rowDelimiter: `\r\n`
- quoteChar: `"`
- escapeChar: `"`

Note: Due to [the following issue](https://github.com/tototoshi/scala-csv/issues/97) in one of our dependencies. The `rowDelimiter` extension will be ignored for any CSV being uploaded. The `rowDelimiter` extension will, however, be observed for downloaded data.
      Also due to [this issue](https://github.com/tototoshi/scala-csv/issues/98) best to avoid non "standard" csv formats. See the `MessageFormatGen.scala` file for examples of which csv formats we test against.

When data is uploaded in CSV format, the headers are interpreted as field names in the same way. As with the Readable JSON format, any string that can be interpreted as another kind of value will be, so for example there's no way to specify the string `"null"`.


## Troubleshooting

First, make sure that the `slamdata/quasar` Github repo is building correctly (the status is displayed at the top of the README).

Then, you can try the following command:

```bash
./sbt test
```

This will ensure that your local version is also passing the tests.

You can also discuss issues [on Discord](https://discord.gg/QNjwCg6).

## Thanks to Sponsors

YourKit supports open source projects with its full-featured Java Profiler. YourKit, LLC is the creator of <a href="https://www.yourkit.com/java/profiler/index.jsp">YourKit Java Profiler</a> and <a href="https://www.yourkit.com/.net/profiler/index.jsp">YourKit .NET Profiler</a>, innovative and intelligent tools for profiling Java and .NET applications.

## Legal

Copyright &copy; 2014 - 2018 SlamData Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
