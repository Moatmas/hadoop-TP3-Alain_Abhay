package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UserRelationMapper extends Mapper<Object, Text, UserPair, Text> {
    private final UserPair userPair = new UserPair();
    private final Text relationFlag = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\t");
        if (tokens.length < 2) return;

        String user = tokens[0];
        String[] relations = tokens[1].split(",");

        List<String> relationList = new ArrayList<>();
        for (String relation : relations) {
            relationList.add(relation);
            // Emit direct connections with a flag "0"
            userPair.set(user, relation);
            relationFlag.set("0");
            context.write(userPair, relationFlag);
        }

        // Emit indirect pairs
        for (int i = 0; i < relationList.size(); i++) {
            for (int j = i + 1; j < relationList.size(); j++) {
                userPair.set(relationList.get(i), relationList.get(j));
                relationFlag.set("1");
                context.write(userPair, relationFlag);
            }
        }
    }
}
