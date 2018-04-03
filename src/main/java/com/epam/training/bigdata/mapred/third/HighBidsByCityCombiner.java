package com.epam.training.bigdata.mapred.third;

import com.epam.training.bigdata.mapred.third.comparable.TextPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Combines <<city, OS>, 1> to <<city, OS>, partial count>.
 */
public class HighBidsByCityCombiner extends Reducer<TextPair, IntWritable, TextPair, IntWritable> {

    private IntWritable countWritable = new IntWritable();

    @Override
    public void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }

        countWritable.set(count);

        context.write(key, countWritable);
    }
}
