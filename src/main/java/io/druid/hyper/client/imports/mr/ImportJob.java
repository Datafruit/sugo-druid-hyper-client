package io.druid.hyper.client.imports.mr;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ImportJob {

    public static final String KEY_DATASORUCE = "datasource";
    public static final String KEY_HMASTER_HOST = "hmaster";
    public static final String KEY_ACTION = "action";
    public static final String KEY_COLUMNS = "columns";

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("No Arguments Given!");
            System.err.println(
                    "hadoop jar druid-hyper-client-1.0.0.jar -jobname -input -output -datasource -hmaster -action -columns");
            System.exit(1);
        }

        Configuration conf = new Configuration();

        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();

        String jobName = remainingArgs[0];
        String input = remainingArgs[1];
        String ouput = remainingArgs[2];
        String datasource = remainingArgs[3];
        String hmaster = remainingArgs[4];
        String action = remainingArgs[5];
        String columns = remainingArgs.length > 6 ? remainingArgs[6] : StringUtils.EMPTY;

        conf.setBoolean(MRJobConfig.MAP_SPECULATIVE, false);
        conf.set(KEY_DATASORUCE, datasource);
        conf.set(KEY_HMASTER_HOST, hmaster);
        conf.set(KEY_ACTION, action);
        conf.set(KEY_COLUMNS, columns);

        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(ImportJob.class);
        job.setMapperClass(ImportMapper.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0);

        // Input Path
        FileInputFormat.addInputPath(job, new Path(input));
        // Output Path
        FileOutputFormat.setOutputPath(job, new Path(ouput));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
