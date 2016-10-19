/*
 *  Copyright 2015-2016 TWO SIGMA OPEN SOURCE, LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.twosigma.flint.timeseries

import com.twosigma.flint.timeseries.summarize.Summary
import com.twosigma.flint.{ SpecUtils, SharedSparkContext }
import org.apache.spark.sql.types._
import org.scalatest.FlatSpec

class SummarizeWindowsSpec extends FlatSpec with SharedSparkContext {

  private val defaultPartitionParallelism: Int = 5

  private val resourceDir: String = "/timeseries/summarizewindows"

  private def from(filename: String, schema: StructType): TimeSeriesRDD =
    SpecUtils.withResource(s"$resourceDir/$filename") { source =>
      CSV.from(
        sqlContext,
        s"file://$source",
        header = true,
        sorted = true,
        schema = schema
      ).repartition(defaultPartitionParallelism)
    }

  "SummarizeWindows" should "pass `SummarizeSingleColumn` test." in {
    val volumeTSRdd = from("Volume.csv", Schema("id" -> IntegerType, "volume" -> LongType))
    val resultsTSRdd = from(
      "SummarizeSingleColumn.results",
      Schema("id" -> IntegerType, "volume" -> LongType, "volume_sum" -> DoubleType)
    )
    val summarizedTSRdd = volumeTSRdd.summarizeWindows(Window.pastAbsoluteTime("100ns"), Summary.sum("volume"))
    assert(summarizedTSRdd.schema == resultsTSRdd.schema)
    assert(summarizedTSRdd.collect().deep == resultsTSRdd.collect().deep)
  }

  it should "pass `SummarizeSingleColumnPerKey` test." in {
    val volumeTSRdd = from("Volume.csv", Schema("id" -> IntegerType, "volume" -> LongType))
    val resultsTSRdd = from(
      "SummarizeSingleColumnPerKey.results",
      Schema("id" -> IntegerType, "volume" -> LongType, "volume_sum" -> DoubleType)
    )
    val summarizedTSRdd = volumeTSRdd.summarizeWindows(Window.pastAbsoluteTime("100ns"), Summary.sum("volume"), Seq("id"))
    assert(summarizedTSRdd.schema == resultsTSRdd.schema)
    assert(summarizedTSRdd.collect().deep == resultsTSRdd.collect().deep)
  }

  it should "pass `SummarizeSingleColumnPerSeqOfKeys` test." in {
    val volumeTSRdd = from(
      "VolumeWithIndustryGroup.csv",
      Schema("id" -> IntegerType, "group" -> IntegerType, "volume" -> LongType)
    )
    val resultsTSRdd = from(
      "SummarizeSingleColumnPerSeqOfKeys.results",
      Schema("id" -> IntegerType, "group" -> IntegerType, "volume" -> LongType, "volume_sum" -> DoubleType)
    )
    val summarizedTSRdd = volumeTSRdd.summarizeWindows(
      Window.pastAbsoluteTime("100ns"), Summary.sum("volume"), Seq("id", "group")
    )
    assert(summarizedTSRdd.schema == resultsTSRdd.schema)
    assert(summarizedTSRdd.collect().deep == resultsTSRdd.collect().deep)
  }
}
