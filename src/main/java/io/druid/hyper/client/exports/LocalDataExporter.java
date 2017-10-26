package io.druid.hyper.client.exports;

import java.io.*;

public class LocalDataExporter extends DataExporter {

    private BufferedWriter writer;

    @Override
    public void init(String filePath) throws IOException {
        File outputFile = new File(filePath);
        File parentDir = outputFile.getParentFile();
        if (!parentDir.exists()) {
            parentDir.mkdirs();
        }
        if (!outputFile.exists()) {
            outputFile.createNewFile();
        }

        OutputStream outputStream = new FileOutputStream(outputFile);
        writer = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"));
    }

    @Override
    public void init(OutputStream outputStream) throws IOException {
        writer = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"));
    }

    @Override
    public void writeRow(String row) throws IOException {
        writer.write(row);
        writer.newLine();
    }

    @Override
    public void flush() throws IOException {
        writer.flush();
    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            writer.close();
        }
    }
}
