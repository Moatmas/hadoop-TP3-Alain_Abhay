package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RecommendationReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<Pair<String, Integer>> recommendations = new ArrayList<>();

        for (Text value : values) {
            String[] parts = value.toString().split(":");
            String user = parts[0];
            int commonRelations = Integer.parseInt(parts[1]);
            recommendations.add(new Pair<>(user, commonRelations));
        }

        recommendations.sort((a, b) -> Integer.compare(b.getValue(), a.getValue()));

        int limit = Math.min(5, recommendations.size());
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < limit; i++) {
            sb.append(recommendations.get(i).getKey()).append(":").append(recommendations.get(i).getValue()).append(",");
        }

        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
        }

        context.write(key, new Text(sb.toString()));
    }
}
