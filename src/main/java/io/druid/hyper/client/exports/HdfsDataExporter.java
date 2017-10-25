package io.druid.hyper.client.exports;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HdfsDataExporter extends DataExporter {
    private static final Logger log = LoggerFactory.getLogger(HdfsDataExporter.class);
    private static final Configuration hadoopConfig = new Configuration();

    private FileSystem FS;
    private FSDataOutputStream outputStream;

    @Override
    public void init(String filePath) throws IOException {
        FS = FileSystem.get(hadoopConfig);
        Path path = new Path(filePath);

        // Make sure parent directory existed.
        Path parentDir = path.getParent();
        FS.mkdirs(parentDir);

        if (FS.exists(path)) {
            outputStream = FS.append(path);
        } else {
            outputStream = FS.create(path, false);
        }
    }

    @Override
    public void writeRow(String row) throws IOException {
        outputStream.write((row + System.lineSeparator()).getBytes());
    }

    @Override
    public void flush() throws IOException {
        outputStream.hsync();
    }

    @Override
    public void close() throws IOException {
        if (FS != null) {
            FS.close();
        }
        if (outputStream != null) {
            outputStream.close();
        }
    }
}
