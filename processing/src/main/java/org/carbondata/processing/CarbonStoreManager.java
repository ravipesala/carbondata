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

package org.carbondata.processing;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.core.carbon.metadata.CarbonMetadata;
import org.carbondata.core.carbon.metadata.converter.SchemaConverter;
import org.carbondata.core.carbon.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.carbondata.core.carbon.metadata.schema.table.TableInfo;
import org.carbondata.core.carbon.path.CarbonStorePath;
import org.carbondata.core.carbon.path.CarbonTablePath;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.reader.ThriftReader;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.writer.ThriftWriter;
import org.carbondata.format.SchemaEvolutionEntry;
import org.carbondata.processing.util.CarbonDataProcessorLogEvent;

import org.apache.thrift.TBase;

/**
 * This class is responsible for managing the store like creating, altering and dropping table.
 */
public class CarbonStoreManager {

  private static LogService LOGGER =
      LogServiceFactory.getLogService(CarbonStoreManager.class.getName());

  private String storePath;

  private Map<String, Long> cubeModifiedTimeStore = new HashMap<String, Long>();

  private boolean useUniquePath;

  private List<CarbonTable> carbonTables;

  /**
   * It intializes the store.
   *
   * @param storePath Store location.
   * @throws IOException
   */
  public CarbonStoreManager(String storePath) throws IOException {
    this.storePath = storePath;
    cubeModifiedTimeStore.put("default", System.currentTimeMillis());
    if ("true".equalsIgnoreCase(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_UNIFIED_STORE_PATH,
            CarbonCommonConstants.CARBON_UNIFIED_STORE_PATH_DEFAULT))) {
      useUniquePath = true;
    }
    carbonTables = loadStore(storePath);
  }

  /**
   * It reads the store and creates list of CarbonTable instances.
   *
   * @param storePath store location
   * @return List<CarbonTable>
   * @throws IOException
   */
  private List<CarbonTable> loadStore(String storePath) throws IOException {
    if (storePath == null) {
      return null;
    }
    FileFactory.FileType fileType = FileFactory.getFileType(storePath);

    CarbonProperties.getInstance().addProperty("carbon.storelocation", storePath);

    List<CarbonTable> metaDataBuffer = new ArrayList<CarbonTable>();

    if (useUniquePath) {
      if (FileFactory.isFileExist(storePath, fileType)) {
        CarbonFile file = FileFactory.getCarbonFile(storePath, fileType);
        CarbonFile[] schemaFolders = file.listFiles();

        for (CarbonFile schemaFolder : schemaFolders) {
          if (schemaFolder.isDirectory()) {
            CarbonFile[] cubeFolders = schemaFolder.listFiles();

            for (CarbonFile cubeFolder : cubeFolders) {
              String schemaPath =
                  new StringBuilder().append(storePath).append("/").append(schemaFolder.getName())
                      .append("/").append(cubeFolder.getName()).toString();
              try {
                fillMetaData(schemaPath, fileType, metaDataBuffer);
                updateDbsUpdatedTime(schemaFolder.getName(), cubeFolder.getName());
              } catch (org.apache.hadoop.security.AccessControlException ex) {
                // Ingnore Access control exception and get only accessible cube details
              }
            }
          }
        }
      }

    } else {
      fillMetaData(storePath, fileType, metaDataBuffer);
      updateDbsUpdatedTime("", "");
    }
    return metaDataBuffer;
  }

  /**
   * It reads the store and creates list of CarbonTable instances.
   *
   * @param basePath
   * @param fileType
   * @param metaDataBuffer
   * @throws IOException
   */
  private void fillMetaData(String basePath, FileFactory.FileType fileType,
      List<CarbonTable> metaDataBuffer) throws IOException {
    String schemasPath = basePath;
    try {

      if (FileFactory.isFileExist(schemasPath, fileType)) {
        CarbonFile file = FileFactory.getCarbonFile(schemasPath, fileType);
        CarbonFile[] schemaFolders = file.listFiles();

        for (CarbonFile schemaFolder : schemaFolders) {
          if (schemaFolder.isDirectory()) {
            String dbName = schemaFolder.getName();
            CarbonFile[] cubeFolders = schemaFolder.listFiles();

            for (CarbonFile cubeFolder : cubeFolders) {
              if (cubeFolder.isDirectory()) {
                CarbonTablePath carbonTablePath = CarbonStorePath.getCarbonTablePath(basePath,
                    new CarbonTableIdentifier(schemaFolder.getName(), cubeFolder.getName()));
                String cubeMetadataFile = carbonTablePath.getSchemaFilePath();

                if (FileFactory.isFileExist(cubeMetadataFile, fileType)) {
                  String tableName = cubeFolder.getName();
                  String cubeUniqueName = schemaFolder.getName() + "_" + cubeFolder.getName();

                  ThriftReader.TBaseCreator createTBase = new ThriftReader.TBaseCreator() {
                    public TBase create() {
                      return new org.carbondata.format.TableInfo();
                    }
                  };
                  ThriftReader thriftReader = new ThriftReader(cubeMetadataFile, createTBase);
                  thriftReader.open();
                  org.carbondata.format.TableInfo tableInfo =
                      (org.carbondata.format.TableInfo) thriftReader.read();
                  thriftReader.close();

                  SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
                  TableInfo wrapperTableInfo =
                      schemaConverter.fromExternalToWrapperTableInfo(tableInfo, dbName, tableName);
                  CarbonTableIdentifier carbonTableIdentifier =
                      new CarbonTableIdentifier(dbName, tableName);
                  String schemaFilePath =
                      CarbonStorePath.getCarbonTablePath(storePath, carbonTableIdentifier)
                          .getSchemaFilePath();
                  wrapperTableInfo
                      .setMetaDataFilepath(CarbonTablePath.getFolderContainingFile(schemaFilePath));
                  CarbonMetadata.getInstance().loadTableMetadata(wrapperTableInfo);
                  metaDataBuffer.add(CarbonMetadata.getInstance().getCarbonTable(cubeUniqueName));
                }
              }
            }
          }
        }

      } else {
        // Create folders and files.
        FileFactory.mkdirs(schemasPath, fileType);

      }
    } catch (FileNotFoundException s) {
      // Create folders and files.
      FileFactory.mkdirs(schemasPath, fileType);
    }
  }

  /**
   * It creates the table in the store.
   *
   * @param tableInfo TableInfo Information of table
   * @throws IOException
   */
  public void createTable(TableInfo tableInfo) throws IOException {
    SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
    org.carbondata.format.TableInfo thriftTableInfo = schemaConverter
        .fromWrapperToExternalTableInfo(tableInfo, tableInfo.getDatabaseName(),
            tableInfo.getFactTable().getTableName());
    SchemaEvolutionEntry schemaEvolutionEntry =
        new SchemaEvolutionEntry(tableInfo.getLastUpdatedTime());
    thriftTableInfo.getFact_table().getSchema_evolution().getSchema_evolution_history()
        .add(schemaEvolutionEntry);

    CarbonTableIdentifier carbonTableIdentifier =
        new CarbonTableIdentifier(tableInfo.getDatabaseName(),
            tableInfo.getFactTable().getTableName());
    CarbonTablePath carbonTablePath =
        CarbonStorePath.getCarbonTablePath(storePath, carbonTableIdentifier);
    String schemaFilePath = carbonTablePath.getSchemaFilePath();
    String schemaMetadataPath = CarbonTablePath.getFolderContainingFile(schemaFilePath);
    tableInfo.setMetaDataFilepath(schemaMetadataPath);
    CarbonMetadata.getInstance().loadTableMetadata(tableInfo);

    FileFactory.FileType fileType = FileFactory.getFileType(schemaMetadataPath);
    if (!FileFactory.isFileExist(schemaMetadataPath, fileType)) {
      FileFactory.mkdirs(schemaMetadataPath, fileType);
    }

    ThriftWriter thriftWriter = new ThriftWriter(schemaFilePath, false);
    thriftWriter.open();
    thriftWriter.write(thriftTableInfo);
    thriftWriter.close();
    LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
        "Cube " + tableInfo.getFactTable().getTableName() + " for schema " + tableInfo
            .getDatabaseName() + " created successfully.");
    updateDbsUpdatedTime(tableInfo.getDatabaseName(), tableInfo.getFactTable().getTableName());
  }

  /**
   * Drops the table from store.
   *
   * @param dbName
   * @param tableName
   */
  public void dropCube(String dbName, String tableName) {

    CarbonTable carbonTable = CarbonMetadata.getInstance().getCarbonTable(dbName + "_" + tableName);

    try {
      if (null != carbonTable) {
        String metadatFilePath = carbonTable.getMetaDataFilepath();
        FileFactory.FileType fileType = FileFactory.getFileType(metadatFilePath);

        if (FileFactory.isFileExist(metadatFilePath, fileType)) {
          CarbonFile file = FileFactory.getCarbonFile(metadatFilePath, fileType);
          // TODO: Use FileFilter to get and delete the files instead of using partionCount
          CarbonUtil.renameCubeForDeletion(100, storePath, dbName, tableName);
          CarbonUtil.deleteFoldersAndFilesSilent(file.getParentFile());
        }

        String partitionLocation = storePath + File.separator + "partition" + File.separator +
            dbName + File.separator + tableName;
        FileFactory.FileType partitionFileType = FileFactory.getFileType(partitionLocation);
        if (FileFactory.isFileExist(partitionLocation, partitionFileType)) {
          CarbonUtil.deleteFoldersAndFiles(
              FileFactory.getCarbonFile(partitionLocation, partitionFileType));
        }
      }
    } catch (Exception ex) {
      // TODO: Throw specific exception like CarbonStorageExcepton.
      LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
          "Error while dropping Table " + tableName + " of " + dbName);
    }

    // TODO: Override equals method in CarbonTable
    carbonTables.remove(carbonTable);
    LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
        "Table " + tableName + " of " + dbName + " dropped successfully.");

  }

  public void alterTable(TableInfo tableInfo) {
    // TODO: Add restructure of table logic here.
  }

  /**
   * Returns all tables from the store.
   *
   * @return
   * @throws IOException
   */
  public List<CarbonTable> getAllTables() throws IOException {
    return getAllTables(null);
  }

  /**
   * Returns all tables located in the dbName of the store
   *
   * @param dbName
   * @return
   * @throws IOException
   */
  public List<CarbonTable> getAllTables(String dbName) throws IOException {
    checkDbsModifiedTimeAndReloadTables();
    List<CarbonTable> carbonTablesFiltered = new ArrayList<CarbonTable>();
    if (dbName != null) {
      for (CarbonTable carbonTable : carbonTables) {
        if (carbonTable.getDatabaseName().equals(dbName)) {
          // TODO : add the copy of table.
          carbonTablesFiltered.add(carbonTable);
        }
      }
    } else {
      carbonTablesFiltered.addAll(carbonTables);
    }

    return carbonTablesFiltered;
  }

  /**
   * Check whether table exists or not in the table.
   *
   * @param dbName
   * @param tableName
   * @return
   */
  public boolean tableExists(String dbName, String tableName) {
    try {
      checkDbsModifiedTimeAndReloadTables();
      for (CarbonTable carbonTable : carbonTables) {
        if (carbonTable.getDatabaseName().equals(dbName) && carbonTable.getFactTableName()
            .equals(tableName)) {
          return true;
        }
      }
    } catch (Exception e) {
      LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, e);
    }
    return false;
  }

  private String getTimestampFile(String dbName, String tableName) {

    StringBuilder timestampFile = new StringBuilder();
    if (useUniquePath) {
      timestampFile.append(storePath).append("/").append(dbName).append(tableName).append("/")
          .append(CarbonCommonConstants.SCHEMAS_MODIFIED_TIME_FILE);
    } else {
      timestampFile.append(storePath).append("/")
          .append(CarbonCommonConstants.SCHEMAS_MODIFIED_TIME_FILE);
    }

    return timestampFile.toString();
  }

  private void updateDbsUpdatedTime(String schemaName, String cubeName) throws IOException {
    String timestampFile = getTimestampFile(schemaName, cubeName);
    FileFactory.FileType fileType = FileFactory.getFileType(timestampFile);

    if (!FileFactory.isFileExist(timestampFile, fileType)) {
      LOGGER.audit("Creating timestamp file");
      FileFactory.createNewFile(timestampFile, fileType);
    }

    touchDbTimestampFile(schemaName, cubeName);

    if (useUniquePath) {
      cubeModifiedTimeStore.put(schemaName + '_' + cubeName,
          FileFactory.getCarbonFile(timestampFile, fileType).getLastModifiedTime());
    } else {
      cubeModifiedTimeStore
          .put("default", FileFactory.getCarbonFile(timestampFile, fileType).getLastModifiedTime());
    }

  }

  private void touchDbTimestampFile(String dbName, String tableName) {
    String timestampFile = getTimestampFile(dbName, tableName);
    FileFactory.getCarbonFile(timestampFile, FileFactory.getFileType(timestampFile))
        .setLastModifiedTime(System.currentTimeMillis());
  }

  private void checkDbsModifiedTimeAndReloadTables() throws IOException {
    if (useUniquePath) {
      for (CarbonTable carbonTable : carbonTables) {
        String timestampFile =
            getTimestampFile(carbonTable.getDatabaseName(), carbonTable.getFactTableName());
        FileFactory.FileType timestampFileType = FileFactory.getFileType(timestampFile);
        if (FileFactory.isFileExist(timestampFile, timestampFileType)) {
          if (!(FileFactory.getCarbonFile(timestampFile, timestampFileType).getLastModifiedTime()
              == cubeModifiedTimeStore.get(carbonTable.getDatabaseName() + "_" +
              carbonTable.getFactTableName()))) {
            refreshCache();
            break;
          }
        }
      }
    } else {
      String timestampFile = getTimestampFile("", "");
      FileFactory.FileType timestampFileType = FileFactory.getFileType(timestampFile);
      if (FileFactory.isFileExist(timestampFile, timestampFileType)) {
        if (!(FileFactory.getCarbonFile(timestampFile, timestampFileType).getLastModifiedTime()
            == cubeModifiedTimeStore.get("default"))) {
          refreshCache();
        }
      }
    }
  }

  /**
   * refresh the tables cache
   * @throws IOException
   */
  public void refreshCache() throws IOException {
    carbonTables = loadStore(storePath);
  }

}
