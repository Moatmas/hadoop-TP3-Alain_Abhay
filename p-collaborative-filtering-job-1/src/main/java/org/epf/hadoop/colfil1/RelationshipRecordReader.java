package org.epf.hadoop.colfil1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;

public class RelationshipRecordReader extends RecordReader<LongWritable, Relationship> {
    private BufferedReader reader;
    private LongWritable currentKey = new LongWritable();
    private Relationship currentValue = new Relationship();
    private long lineNumber = 0;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        Path filePath = ((org.apache.hadoop.mapreduce.lib.input.FileSplit) split).getPath();
        FileSystem fs = filePath.getFileSystem(context.getConfiguration());
        reader = new BufferedReader(new InputStreamReader(fs.open(filePath)));
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        String line = reader.readLine();
        if (line == null) {
            return false;
        }
        lineNumber++;

        line = line.trim();
        if (line.isEmpty()) {
            return true;
        }

        String[] parts = line.split(";")[0].split("<->");

        if (parts.length < 2) {
            System.out.println("Ligne malformée (pas deux éléments séparés par <->) : " + line);
            return true;
        }

        currentKey.set(lineNumber);
        currentValue.setId1(parts[0]);
        currentValue.setId2(parts[1]);

        return true;
    }

    @Override
    public LongWritable getCurrentKey() {
        return currentKey;
    }

    @Override
    public Relationship getCurrentValue() {
        return currentValue;
    }

    @Override
    public float getProgress() {
        return 0; // Simplification
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
