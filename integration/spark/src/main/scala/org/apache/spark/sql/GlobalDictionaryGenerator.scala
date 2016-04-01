/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql

import org.apache.spark.{HashPartitioner, Partitioner};
import org.carbondata.core.carbon.CarbonDef.Dimension
import org.carbondata.integration.spark.load.CarbonLoadModel

import scala.collection.mutable

/**
 * It generates dictionary for each column field.
 */
class GlobalDictionaryGenerator extends Serializable {


  def generateGlobalSurrogates(sqlContext: SQLContext,
                               carbonLoadModel:CarbonLoadModel,
                               storeLocation: String) {


    val columns = new mutable.ArrayBuffer[String]
    carbonLoadModel.getSchema.cubes(0).dimensions.foreach{dim =>
      dim.asInstanceOf[Dimension].hierarchies(0).levels.map{level =>
        columns += level.column
      }
    }
    //TODO : Read the dimension files to generate dictionary.
    //Read the data using Spark-csv datasource.
    val df = sqlContext.read.format("com.databricks.spark.csv").
    option("header", carbonLoadModel.getCsvHeader.isEmpty.toString).
    option("delimiter", carbonLoadModel.getCsvDelimiter).
    load(carbonLoadModel.getFactFilePath).select(columns.toSeq.map(new Column(_)) : _*)

    //Just a dummy
    val value = new GlobalSurrogateValue

    //Create the keys for each column field with column number as partitionID.And flatten it.
    df.map { s =>
      s.toSeq.zipWithIndex.map{ r =>
        (new GlobalSurrogateKey(r._1.toString, r._2), value)
      }
    }.flatMap(a => a).

    // Partition by using column number so that each column data go to one individual partition.
    partitionBy(new CarbonPartitioner(columns.length)).mapPartitions { iter =>
      val set = new mutable.HashSet[String]
      while (iter.hasNext) {
        set.add(iter.next()._1.key)
      }
      //TODO : Sort the data and do merge sort to filter out the existing dictionsry names.
      //We can write the data to disk for individual partition.Before writing we can merge the already
      //existed surrogates and generate for new data.
      set.toSeq.sorted.iterator
    }.collect.foreach(println)
  }

}

class GlobalSurrogateKey(val key: String, val partition: Int)
  extends Serializable with Comparable[GlobalSurrogateKey] {

  override def compareTo(o: GlobalSurrogateKey) = {
    key.compareTo(o.key)
  }

  override def equals(that: Any) = {
    if (that != null) {
      val other = that.asInstanceOf[GlobalSurrogateKey]
      key.equals(other.key) && partition == other.partition
    }
    false
  }

  override def hashCode() = {
    key.hashCode
  }

  override def toString = {
    key + " : " + partition
  }
}

class GlobalSurrogateValue() extends Serializable {
  override def toString = {
    ""
  }
}

class CarbonPartitioner(partitions: Int) extends Partitioner {
  def getPartition(key: Any): Int = {
    val other = key.asInstanceOf[GlobalSurrogateKey]
    other.partition
  }

  override def equals(other: Any): Boolean = other match {
    case h: HashPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  def numPartitions: Int = partitions

  override def hashCode: Int = numPartitions

}

