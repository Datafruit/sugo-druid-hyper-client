package io.druid.hyper.hive.io;

import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class DruidQueryBasedInputFormat implements InputFormat {
    @Override
    public InputSplit[] getSplits(JobConf jobConf, int i) throws IOException {
        return new InputSplit[0];
    }

    @Override
    public RecordReader getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
        return null;
    }
}
