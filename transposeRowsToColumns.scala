def transposeRowsToColumns(table:ArrayBuffer[List[String]]):List[List[String]] = {

    /*
     * The procedure to change columns to rows -> RDD[Seq[(Int, Double)]]
     * Key is the number of column
     */

    //    val t1 = System.currentTimeMillis()
    val byColumnAndRow = table.drop(0).zipWithIndex.flatMap {
      case (row, rowIndex) => row.zipWithIndex.map {
        case (number, columnIndex) => columnIndex -> (rowIndex, columnIndex + 1, number)
      }
    }

    // Build up the transposed matrix. Group and sort by column index first.
    val byColumn = byColumnAndRow.groupBy(_._1).toList.sortBy(_._1)

    // Then sort by row index.
    val transposed = byColumn.map({
      indexedRow => (indexedRow._2.toSeq.sortBy(_._1).map(x => x._2._3))
    })
      .map(x => x.toList)

    transposed

}
