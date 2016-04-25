package org.carbondata.processing;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.carbon.metadata.encoder.Encoding;
import org.carbondata.core.carbon.metadata.schema.SchemaEvolution;
import org.carbondata.core.carbon.metadata.schema.SchemaEvolutionEntry;
import org.carbondata.core.carbon.metadata.schema.table.TableInfo;
import org.carbondata.core.carbon.metadata.schema.table.TableSchema;
import org.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CarbonStoreManagerTest {

  static TableInfo tableInfo;

  @Before public void setUpBeforeClass() throws Exception {
    tableInfo = new TableInfo();
    TableSchema tableSchema = new TableSchema();
    SchemaEvolution schemaEvol = new SchemaEvolution();
    schemaEvol.setSchemaEvolutionEntryList(new ArrayList<SchemaEvolutionEntry>());
    tableSchema.setTableId(1);
    tableSchema.setTableName("testtable");
    List<ColumnSchema> columnSchemas = new ArrayList<ColumnSchema>();
    ColumnSchema column1 = new ColumnSchema();
    column1.setColumnName("column1");
    column1.setDataType(DataType.STRING);
    column1.setColumnar(true);
    column1.setColumnUniqueId(UUID.randomUUID().toString());
    column1.setEncodingList(new ArrayList<Encoding>());
    ColumnSchema column2 = new ColumnSchema();
    column2.setColumnName("column2");
    column2.setDataType(DataType.STRING);
    column2.setColumnar(true);
    column2.setColumnUniqueId(UUID.randomUUID().toString());
    column2.setEncodingList(new ArrayList<Encoding>());
    ColumnSchema column3 = new ColumnSchema();
    column3.setColumnName("column1");
    column3.setDataType(DataType.DOUBLE);
    column3.setColumnar(true);
    column3.setColumnUniqueId(UUID.randomUUID().toString());
    column3.setEncodingList(new ArrayList<Encoding>());
    columnSchemas.add(column1);
    columnSchemas.add(column2);
    columnSchemas.add(column3);
    tableSchema.setListOfColumns(columnSchemas);
    tableSchema.setSchemaEvalution(schemaEvol);
    tableInfo.setDatabaseName("testdb");
    tableInfo.setTableUniqueName(tableInfo.getDatabaseName() + "_" + tableSchema.getTableName());
    tableInfo.setLastUpdatedTime(System.currentTimeMillis());
    tableInfo.setFactTable(tableSchema);
    tableInfo.setAggregateTableList(new ArrayList<TableSchema>());
  }

  @Test public void test_CreateTable() {
    File file = new File("target/storemanager");
    file.mkdirs();
    try {
      CarbonStoreManager carbonStoreManager = new CarbonStoreManager(file.getAbsolutePath());
      carbonStoreManager.createTable(tableInfo);
    } catch (Exception e) {
      Assert.assertFalse(e.getMessage(), false);
    }
  }
}
