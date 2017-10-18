package io.druid.hyper.client.imports.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ImportJob {

    public static final String KEY_DATASORUCE = "datasource";
    public static final String KEY_HMASTER_HOST = "hmaster";
    public static final String KEY_ACTION = "action";
    public static final String KEY_COLUMNS = "columns";

    public static void main(String[] args) throws Exception {

        // Input path
        String dst = args[0];
        String dstOut = args[1];

        Configuration hadoopConfig = new Configuration();
        hadoopConfig.set(KEY_DATASORUCE, args[2]);
        hadoopConfig.set(KEY_HMASTER_HOST, args[3]);
        hadoopConfig.set(KEY_ACTION, args[4]);
        hadoopConfig.set(KEY_COLUMNS, args[5]);

        Job job = Job.getInstance(hadoopConfig);
        job.setJarByClass(ImportJob.class);
        job.setMapperClass(ImportMapper.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(dst));
        FileOutputFormat.setOutputPath(job, new Path(dstOut));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
