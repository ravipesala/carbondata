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

import java.io.File
import java.sql.{DriverManager, ResultSet}

import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.{JdbcRDDExt, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{Analyzer, OverrideCatalog, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.csv.CsvRDD
import org.apache.spark.sql.cubemodel.{LoadCubeAPI, MergeCube, PartitionData, Partitioner}
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.hive._
import org.apache.spark.sql.jdbc.JdbcResultSetRDD
import org.apache.spark.sql.types.StructType
import org.carbondata.common.logging.LogServiceFactory
import org.carbondata.integration.spark.agg.FlattenExpr
import org.carbondata.integration.spark.rdd.{CarbonDataFrameRDD, SchemaRDDExt}
import org.carbondata.integration.spark.util.CarbonSparkInterFaceLogEvent
import org.carbondata.query.aggregator.MeasureAggregator
import org.carbondata.query.querystats.{QueryDetail, QueryStatsCollector}

import scala.language.implicitConversions

class CarbonContext(sc: SparkContext, metadataPath: String) extends HiveContext(sc) {
  self =>

  def this(sc: SparkContext) {
    this(sc, new File("./").getAbsolutePath)
  }

  var lastSchemaUpdatedTime = System.currentTimeMillis()

  @transient
  override lazy val catalog =
    new CarbonMetastoreCatalog(self, metadataPath, metadataHive) with OverrideCatalog

  @transient
  override protected[sql] lazy val analyzer =
    new Analyzer(catalog, functionRegistry, conf)

  override protected[sql] def dialectClassName = classOf[CarbonSQLDialect].getCanonicalName

  experimental.extraStrategies = CarbonStrategy.getStrategy(self) :: Nil

  def loadSchema(schemaPath: String, encrypted: Boolean = true, aggTablesGen: Boolean = true, partitioner: Partitioner = null) {
    CarbonContext.updateCarbonPorpertiesPath(this)
    CarbonEnv.getInstance(this).carbonCatalog.loadCube(schemaPath, encrypted, aggTablesGen, partitioner)(this)
  }

  def updateSchema(schemaPath: String, encrypted: Boolean = true, aggTablesGen: Boolean = false) {
    CarbonEnv.getInstance(this).carbonCatalog.updateCube(schemaPath, encrypted, aggTablesGen)(this)
  }

  def cubeExists(schemaName: String, cubeName: String): Boolean = {
    CarbonEnv.getInstance(this).carbonCatalog.cubeExists(Seq(schemaName, cubeName))(this)
  }

  def loadData(schemaName: String = null, cubeName: String, dataPath: String, dimFilesPath: String = null) {
    var schemaNameLocal = schemaName
    if (schemaNameLocal == null) {
      schemaNameLocal = "default"
    }
    var dimFilesPathLocal = dimFilesPath
    if (dimFilesPath == null) {
      dimFilesPathLocal = dataPath
    }
    CarbonContext.updateCarbonPorpertiesPath(this)
    LoadCubeAPI(schemaNameLocal, cubeName, dataPath, dimFilesPathLocal, null).run(this)
  }

  def mergeData(schemaName: String = null, cubeName: String, tableName: String) {
    var schemaNameLocal = schemaName
    if (schemaNameLocal == null) {
      schemaNameLocal = cubeName
    }
    MergeCube(schemaNameLocal, cubeName, tableName).run(this)
  }

  @DeveloperApi
  implicit def toAggregates(aggregate: MeasureAggregator): Double = aggregate.getDoubleValue()

  /**
    * Loads a CSV file (according to RFC 4180) and returns the result as a [[SchemaRDD]].
    *
    * NOTE: If there are new line characters inside quoted fields this method may fail to
    * parse correctly, because the two lines may be in different partitions. Use
    * [[SQLContext#csvRDD]] to parse such files.
    *
    * @param path      path to input file
    * @param delimiter Optional delimiter (default is comma)
    * @param quote     Optional quote character or string (default is '"')
    * @param schema    optional StructType object to specify schema (field names and types). This will
    *                  override field names if header is used
    * @param header    Optional flag to indicate first line of each file is the header
    *                  (default is false)
    */
  def csvFile(
      path: String,
      delimiter: String = ",",
      quote: Char = '"',
      schema: StructType = null,
      header: Boolean = false): SchemaRDD = {
    val csv = sparkContext.textFile(path)
    csvRDD(csv, delimiter, quote, schema, header)
  }

  /**
    * Parses an RDD of String as a CSV (according to RFC 4180) and returns the result as a
    * [[SchemaRDD]].
    *
    * NOTE: If there are new line characters inside quoted fields, use
    * [[SparkContext#wholeTextFiles]] to read each file into a single partition.
    *
    * @param csv       input RDD
    * @param delimiter Optional delimiter (default is comma)
    * @param quote     Optional quote character of strig (default is '"')
    * @param schema    optional StructType object to specify schema (field names and types). This will
    *                  override field names if header is used
    * @param header    Optional flag to indicate first line of each file is the hader
    *                  (default is false)
    */
  def csvRDD(
              csv: RDD[String],
              delimiter: String = ",",
              quote: Char = '"',
              schema: StructType = null,
              header: Boolean = false): SchemaRDD = {
    new SchemaRDD(this, CsvRDD.inferSchema(csv, delimiter, quote, schema, header)(this))
  }

  /**
    * Creates a SchemaRDD from an RDD of case classes.
    *
    * @group userf
    */
  implicit def createSchemaExtRDD(rdd: SchemaRDD) =
    new SchemaRDDExt(rdd.sqlContext, rdd.logicalPlan)

  override def sql(sql: String): SchemaRDD = {
    //queryId will be unique for each query, creting query detail holder
    val queryStatsCollector: QueryStatsCollector = QueryStatsCollector.getInstance
    val queryId: String = System.nanoTime() + ""
    val queryDetail: QueryDetail = new QueryDetail(queryId)
    queryStatsCollector.addQueryStats(queryId, queryDetail)
    this.setConf("queryId", queryId)

    CarbonContext.updateCarbonPorpertiesPath(this)
    val sqlString = sql.toUpperCase
    val LOGGER = LogServiceFactory.getLogService(CarbonContext.getClass().getName())
    LOGGER.info(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, s"Query [$sqlString]")
    val logicPlan: LogicalPlan = parseSql(sql)
    //val result = new SchemaRDD(this,logicPlan)
    val result = new CarbonDataFrameRDD(sql: String, this, logicPlan)

    // We force query optimization to happen right away instead of letting it happen lazily like
    // when using the query DSL.  This is so DDL commands behave as expected.  This is only
    // generates the RDD lineage for DML queries, but do not perform any execution.
    //    result.queryExecution.toRdd
    result
  }

  /**
    * All the measure objects inside SchemaRDD will be flattened
    */

  def flattenRDD(rdd: SchemaRDD) = {
    val fields = rdd.schema.fields.map { f =>
      new Column(FlattenExpr(UnresolvedAttribute(f.name)))
    }
    println(fields)
    rdd.as(Symbol("carbon_flatten")).select(fields: _*)
  }

  implicit def dataset(name: String): SchemaRDDExt = {
    table(name).as(Symbol(name))
  }

  /** Caches the specified table in-memory. */
  override def cacheTable(tableName: String): Unit = {
    //todo:
  }

  /**
    * Loads from JDBC, returning the ResultSet as a [[SchemaRDD]].
    * It gets MetaData from ResultSet of PreparedStatement to determine the schema.
    *
    * @group userf
    */
  def jdbcResultSet(
                     connectString: String,
                     sql: String): SchemaRDD = {
    jdbcResultSet(connectString, "", "", sql, 0, 0, 1)
  }

  def jdbcResultSet(
                     connectString: String,
                     username: String,
                     password: String,
                     sql: String): SchemaRDD = {
    jdbcResultSet(connectString, username, password, sql, 0, 0, 1)
  }

  def jdbcResultSet(
                     connectString: String,
                     sql: String,
                     lowerBound: Long,
                     upperBound: Long,
                     numPartitions: Int): SchemaRDD = {
    jdbcResultSet(connectString, "", "", sql, lowerBound, upperBound, numPartitions)
  }

  def jdbcResultSet(
                     connectString: String,
                     username: String,
                     password: String,
                     sql: String,
                     lowerBound: Long,
                     upperBound: Long,
                     numPartitions: Int): SchemaRDD = {
    val resultSetRDD = new JdbcRDDExt(
      sparkContext,
      () => {
        DriverManager.getConnection(connectString, username, password)
      },
      sql, lowerBound, upperBound, numPartitions,
      (r: ResultSet) => r
    )
    //new SchemaRDD(this, JdbcResultSetRDD.inferSchema(resultSetRDD))
    val appliedSchema = JdbcResultSetRDD.inferSchema(resultSetRDD)
    val rowRDD = JdbcResultSetRDD.jdbcResultSetToRow(resultSetRDD, appliedSchema)
    applySchema1(rowRDD, appliedSchema)
  }

  def applySchema1(rowRDD: RDD[InternalRow], schema: StructType): DataFrame = {
    // TODO: use MutableProjection when rowRDD is another SchemaRDD and the applied
    // schema differs from the existing schema on any field data type.
    val attributes = schema.fields.map(f => AttributeReference(f.name, f.dataType, f.nullable)())
    val logicalPlan = LogicalRDD(attributes, rowRDD)(this)
    new DataFrame(this, logicalPlan)
  }


}

object CarbonContext {
  /**
    * @param schemaName - Schema Name
    * @param cubeName   - Cube Name
    * @param factPath   - Raw CSV data path
    * @param targetPath - Target path where the file will be split as per partition
    * @param delimiter  - default file delimiter is comma(,)
    * @param quoteChar  - default quote character used in Raw CSV file, Default quote
    *                   character is double quote(")
    * @param fileHeader - Header should be passed if not available in Raw CSV File, else pass null, Header will be read from CSV
    * @param escapeChar - This parameter by default will be null, there wont be any validation if default escape
    *                   character(\) is found on the RawCSV file
    * @param multiLine  - This parameter will be check for end of quote character if escape character & quote character is set.
    *                   if set as false, it will check for end of quote character within the line and skips only 1 line if end of quote not found
    *                   if set as true, By default it will check for 10000 characters in multiple lines for end of quote & skip all lines if end of quote not found.
    */
  final def partitionData(
                           schemaName: String = null,
                           cubeName: String,
                           factPath: String,
                           targetPath: String,
                           delimiter: String = ",",
                           quoteChar: String = "\"",
                           fileHeader: String = null,
                           escapeChar: String = null,
                           multiLine: Boolean = false)(hiveContext: HiveContext): String = {
    updateCarbonPorpertiesPath(hiveContext)
    var schemaNameLocal = schemaName
    if (schemaNameLocal == null) {
      schemaNameLocal = "default"
    }
    val partitionDataClass = PartitionData(schemaName, cubeName, factPath, targetPath, delimiter, quoteChar, fileHeader, escapeChar, multiLine)
    partitionDataClass.run(hiveContext)
    partitionDataClass.partitionStatus
  }

  final def updateCarbonPorpertiesPath(hiveContext: HiveContext) {
    val carbonPropertiesFilePath = hiveContext.getConf("carbon.properties.filepath", null)
    val systemcarbonPropertiesFilePath = System.getProperty("carbon.properties.filepath", null);
    if (null != carbonPropertiesFilePath && null == systemcarbonPropertiesFilePath) {
      System.setProperty("carbon.properties.filepath", carbonPropertiesFilePath + "/" + "carbon.properties")
    }
  }

}
