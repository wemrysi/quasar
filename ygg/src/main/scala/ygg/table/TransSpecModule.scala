package ygg.table

trait TransSpecModule {
  object trans extends TransSpecClasses
  import trans.{ Leaf, Source, Filter, Map1, Equal, root }

  implicit def liftF1(f1: CF1): CF1Like
  implicit def liftF2(f2: CF2): CF2Like
  def lookupF1(namespace: List[String], name: String): CF1
  def lookupF2(namespace: List[String], name: String): CF2

  object F1Expr {
    def negate         = lookupF1(Nil, "negate")
    def coerceToDouble = lookupF1(Nil, "coerceToDouble")
    def moduloTwo      = lookupF2(Nil, "mod") applyr CLong(2)
    def equalsZero     = lookupF2(Nil, "eq") applyr CLong(0)
    def isEven         = moduloTwo andThen equalsZero
  }
  object Fn {
    def source                    = Leaf(Source)
    def valueIsEven(name: String) = Map1(root select name, F1Expr.isEven)
    def constantTrue              = Filter(source, Equal(source, source))
  }
  trait CF1Like {
    def compose(f1: CF1): CF1
    def andThen(f1: CF1): CF1
  }
  trait CF2Like {
    def applyl(cv: CValue): CF1
    def applyr(cv: CValue): CF1
    def andThen(f1: CF1): CF2
  }
}

object TransSpecModule {
  object paths {
    val Key     = CPathField("key")
    val Value   = CPathField("value")
    val Group   = CPathField("group")
    val SortKey = CPathField("sortkey")
  }

  sealed trait Definedness
  case object AnyDefined extends Definedness
  case object AllDefined extends Definedness
}
