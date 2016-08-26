package xygg

import sbt._, Keys._

object Boilerplate {
  def boilerplateTypes = List("String", "Int", "Long", "Double")
  // def boilerplateTypes = List("String", "Int", "Long", "Double", "Boolean", "BigDecimal", "Period")
  def readTemplate(name: String) = Def setting IO.read(baseDirectory.value / "templates" / s"${name}.scala")

  def substitute(template: String)(subst: (String, String)*): String =
    subst.foldLeft(template) {
      case (res, (from, to)) =>
        res.replaceAllLiterally("${{" + from + "}}", to)
    }

  val generate = Def.task[Seq[File]] {
    val templ = readTemplate("columnImpl").value
    def columnImpl(tp: String): String = substitute(templ)(
      "ROWID" -> "Int",
      "TYPE" -> tp
    )

    val pre  = substitute(readTemplate("columnPreamble").value)("ROWID" -> "Int")
    val dir  = (sourceManaged in Compile).value
    val file = dir / "ColumnHasher.scala"
    val code = (boilerplateTypes map columnImpl).mkString(pre, "\n\n", "")

    IO.write(file, code)
    Seq(file)
  }
}
