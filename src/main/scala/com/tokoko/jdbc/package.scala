package com.tokoko

import org.apache.spark.sql.DataFrameReader

package object jdbc {

  implicit def createDataFrameReaderImplicits(df: DataFrameReader): DataFrameReaderImplicits =
    new DataFrameReaderImplicits(df)

}
