package org.epf.hadoop.colfil1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class RelationshipInputFormat extends FileInputFormat<LongWritable, Relationship> {
    @Override
    public RecordReader<LongWritable, Relationship> createRecordReader(
            InputSplit split, TaskAttemptContext context) {
        return new RelationshipRecordReader();
    }
}
