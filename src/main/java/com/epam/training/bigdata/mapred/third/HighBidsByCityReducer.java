package com.epam.training.bigdata.mapred.third;

import com.epam.training.bigdata.mapred.third.comparable.TextPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reduces <<city, OS>, count> to <city, total count>, where count is either 1 from mapper
 * or partial count from combiner.
 */
public class HighBidsByCityReducer extends Reducer<TextPair, IntWritable, Text, IntWritable> {

    private Text city = new Text();
    private IntWritable countWritable = new IntWritable();

    @Override
    public void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }

        city = key.getFirst();
        countWritable.set(count);

        context.write(city, countWritable);
    }
}
