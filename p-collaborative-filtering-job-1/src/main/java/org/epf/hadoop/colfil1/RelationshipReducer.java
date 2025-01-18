package org.epf.hadoop.colfil1;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;


import java.io.IOException;

public  class RelationshipReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder relations = new StringBuilder();
        for (Text value : values) {
            if (relations.length() > 0) {
                relations.append(",");
            }
            relations.append(value.toString());
        }
        context.write(key, new Text(relations.toString()));
    }
}

