/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.druid.hyper.hive;

import io.druid.hyper.hive.io.Constants;
import io.druid.hyper.hive.io.DruidOutputFormat;
import io.druid.hyper.hive.io.DruidQueryBasedInputFormat;
import io.druid.hyper.hive.serde.DruidSerDe;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * DruidStorageHandler provides a HiveStorageHandler implementation for Druid.
 */
@SuppressWarnings({ "deprecation", "rawtypes" })
public class DruidStorageHandler  implements HiveStorageHandler, HiveMetaHook{

  protected static final Logger LOG = LoggerFactory.getLogger(DruidStorageHandler.class);

  private Configuration conf;

  public DruidStorageHandler() {

  }


  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return DruidQueryBasedInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return DruidOutputFormat.class;
  }

  @Override
  public Class<? extends SerDe> getSerDeClass() {
    return DruidSerDe.class;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return this;
  }

  @Override
  public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
    return new DefaultHiveAuthorizationProvider();
  }

  @Override
  public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties
  ) {

  }

  @Override
  public void preCreateTable(Table table) throws MetaException {
    // Do safety checks
    if (MetaStoreUtils.isExternalTable(table) && !StringUtils
            .isEmpty(table.getSd().getLocation())) {
      throw new MetaException("LOCATION may not be specified for Druid");
    }

    if (table.getPartitionKeysSize() != 0) {
      throw new MetaException("PARTITIONED BY may not be specified for Druid");
    }
    if (table.getSd().getBucketColsSize() != 0) {
      throw new MetaException("CLUSTERED BY may not be specified for Druid");
    }
    String dataSourceName = table.getParameters().get(Constants.DRUID_DATA_SOURCE);
    if (MetaStoreUtils.isExternalTable(table)) {
      return;
    }

  }

  @Override
  public void rollbackCreateTable(Table table) throws MetaException {

  }

  @Override
  public void commitCreateTable(Table table) throws MetaException {

  }



  @Override
  public void preDropTable(Table table) throws MetaException {
    // Nothing to do
  }

  @Override
  public void rollbackDropTable(Table table) throws MetaException {
    // Nothing to do
  }

  @Override
  public void commitDropTable(Table table, boolean deleteData) throws MetaException {

  }


  @Override
  public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties
  ) {
    jobProperties.put(Constants.DRUID_SEGMENT_VERSION, new DateTime().toString());
  }

  @Override
  public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties
  ) {

  }

  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
//    try {
//      DruidStorageHandlerUtils.addDependencyJars(jobConf, DruidRecordWriter.class);
//    } catch (IOException e) {
//      Throwables.propagate(e);
//    }
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public String toString() {
    return Constants.DRUID_HIVE_STORAGE_HANDLER_ID;
  }

}
