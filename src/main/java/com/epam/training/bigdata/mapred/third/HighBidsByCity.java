package com.epam.training.bigdata.mapred.third;

import com.epam.training.bigdata.mapred.third.comparable.TextPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Driver class for a High Bids by City MR job. Main method should be run with input and output paths,
 * as well as with a path to cities dictionary and number of reducers as parameters.
 */
public class HighBidsByCity {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "High Bids by City");
        job.setJarByClass(HighBidsByCity.class);

        //Adding input and output file paths to job based on the arguments passed
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        //Setting mapper, reducer, combiner and partitioner classes
        job.setMapperClass(HighBidsByCityMapper.class);
        job.setCombinerClass(HighBidsByCityCombiner.class);
        job.setPartitionerClass(OSPartitioner.class);
        job.setReducerClass(HighBidsByCityReducer.class);

        job.addCacheFile(new Path(args[2]).toUri());
        job.setNumReduceTasks(Integer.valueOf(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
