package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RecommendationMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");

        if (parts.length >= 3) {
            String user1 = parts[0].trim();
            String user2 = parts[1].trim();
            int commonRelations;
            try {
                commonRelations = Integer.parseInt(parts[2].trim());
            } catch (NumberFormatException e) {
                return;
            }

            context.write(new Text(user1), new Text(user2 + ":" + commonRelations));
            context.write(new Text(user2), new Text(user1 + ":" + commonRelations));
        } else {
            System.err.println("Ligne mal formatée: " + value.toString());
        }
    }
}
