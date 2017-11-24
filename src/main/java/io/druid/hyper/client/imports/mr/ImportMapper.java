package io.druid.hyper.client.imports.mr;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.hyper.client.imports.DataSender;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ImportMapper extends Mapper<LongWritable, Text, LongWritable, NullWritable> {

    private static final Logger log = LoggerFactory.getLogger(ImportMapper.class);

    private String hmaster;
    private String dataSource;
    private String action;
    private List<String> columns;
    private DataSender sender;

    enum SendCounter {
        FAILED, SUCCESS
    }

    protected void setup(final Context context) throws IOException, InterruptedException {
        hmaster = context.getConfiguration().get(ImportJob.KEY_HMASTER_HOST);
        dataSource = context.getConfiguration().get(ImportJob.KEY_DATASORUCE);
        action = context.getConfiguration().get(ImportJob.KEY_ACTION);

        String columnsStr = context.getConfiguration().get(ImportJob.KEY_COLUMNS);
        columns = Lists.newArrayList(Splitter.on(",").trimResults().split(columnsStr));

        sender = DataSender.builder()
                    .toServer(hmaster)
                    .ofDataSource(dataSource)
                    .withReporter(context)
                    .build();
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

            if ("U".equalsIgnoreCase(action)) {
                sender.update(columns,
                        Lists.newArrayList(
                                Iterables.transform(
                                        Splitter.on("\001").trimResults().split(line),
                                        new Function<String, Object>() {
                                            @Nullable
                                            @Override
                                            public Object apply(@Nullable String input) {
                                                return input;
                                            }
                                        }
                                )
                        )
                );
            } else {
                sender.add(line);
            }
        } catch (Exception ex) {
            log.error("Failed to send the content", ex);
            context.getCounter(SendCounter.FAILED).increment(1);
        }
    }
}
