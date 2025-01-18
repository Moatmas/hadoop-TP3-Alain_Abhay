package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UserRelationReducer extends Reducer<UserPair, Text, UserPair, Text> {
    private final Text outputValue = new Text();

    @Override
    protected void reduce(UserPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int relationCount = 0;
        boolean isDirectConnection = false;

        for (Text value : values) {
            if (value.toString().equals("0")) {
                isDirectConnection = true;
            } else {
                relationCount++;
            }
        }

        if (!isDirectConnection && relationCount > 0) {
            outputValue.set(String.valueOf(relationCount));
            context.write(key, outputValue);
        }
    }
}
