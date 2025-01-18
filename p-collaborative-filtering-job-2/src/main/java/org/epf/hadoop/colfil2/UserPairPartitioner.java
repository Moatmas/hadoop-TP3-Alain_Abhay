package org.epf.hadoop.colfil2;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;


public class UserPairPartitioner extends Partitioner<UserPair, Text> {
    @Override
    public int getPartition(UserPair key, Text value, int numPartitions) {
        return Math.abs(key.getUser1().hashCode()) % numPartitions;
    }
}
