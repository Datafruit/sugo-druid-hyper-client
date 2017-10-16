package io.druid.hyper.client.imports.mr;

import io.druid.hyper.client.imports.DataSender;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ImportMapper extends Mapper<LongWritable, Text, LongWritable, NullWritable> {

    private static final Logger log = LoggerFactory.getLogger(ImportMapper.class);

    private String hmaster;
    private String dataSource;
    private DataSender sender;

    enum SendCounter {
        FAILED, SUCCESS
    }

    protected void setup(final Context context) throws IOException, InterruptedException {
        hmaster = context.getConfiguration().get(ImportJob.KEY_HMASTER_HOST);
        dataSource = context.getConfiguration().get(ImportJob.KEY_DATASORUCE);
        sender = DataSender.builder().toServer(hmaster).ofDataSource(dataSource).build();
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (sender != null) {
            sender.close();
        }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.getCounter(SendCounter.SUCCESS).increment(1);

        try {
            String line = value.toString();
            line = line.replaceAll("\n" , "");
            sender.add(line);
        } catch (Exception ex) {
            log.error("Failed to send the content", ex);
            context.getCounter(SendCounter.FAILED).increment(1);
        }
    }
}