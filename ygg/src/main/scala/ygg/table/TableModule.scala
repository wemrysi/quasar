package ygg.table

trait TableModule {
  outer =>

  val Table: TableCompanion
  type Table <: ygg.table.Table
  type TableCompanion <: ygg.table.TableCompanion[Table]
}

trait TableModuleColumnar extends TableModule {
  type Table <: ygg.table.ColumnarTable
  type TableCompanion <: ygg.table.ColumnarTableCompanion[Table]
}
