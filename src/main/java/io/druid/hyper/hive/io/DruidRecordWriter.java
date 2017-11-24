/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.hyper.hive.io;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.druid.hyper.client.imports.DataSender;
import io.druid.hyper.hive.serde.DruidWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class DruidRecordWriter implements RecordWriter<NullWritable, DruidWritable>,
        org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter {

  protected static final Logger LOG = LoggerFactory.getLogger(DruidRecordWriter.class);

  private final String datasource;
  private final DataSender dataSender;

  public DruidRecordWriter(
          Properties tableProperties,
          Configuration conf
  ) {
    this.datasource = Preconditions.checkNotNull(tableProperties.getProperty(Constants.DRUID_DATA_SOURCE), "data source is null");

    String masterStr = conf.get(Constants.HIVE_DRUID_HMASTER_DEFAULT_ADDRESS);
    masterStr = Strings.isNullOrEmpty(masterStr) ? tableProperties.getProperty(Constants.HIVE_DRUID_HMASTER_DEFAULT_ADDRESS) : masterStr;
    Preconditions.checkNotNull(masterStr, "hmaster is null");
    Iterable<String> masterIt = Splitter.on(",").omitEmptyStrings().trimResults().split(masterStr);
    List<String> masters = Lists.newArrayList(masterIt);

    dataSender = DataSender.builder().toServer(masters.get(0)).ofDataSource(datasource).build();
  }

  @Override
  public void write(Writable w) throws IOException {
    DruidWritable record = (DruidWritable) w;
    Map<String, Object> kvs =  record.getValue();
    kvs.remove("__time");
    try {
      dataSender.update(kvs);
    } catch (Exception e) {
      LOG.error("Write data to data source: " + datasource + " error. Details: ", e);
    }
  }

  @Override
  public void close(boolean abort) throws IOException {
    dataSender.close();
  }

  @Override
  public void write(NullWritable key, DruidWritable value) throws IOException {
    this.write(value);
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    dataSender.setReporter(reporter);
    this.close(true);
  }

}
