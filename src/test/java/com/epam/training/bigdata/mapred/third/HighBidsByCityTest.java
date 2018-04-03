package com.epam.training.bigdata.mapred.third;

import com.epam.training.bigdata.mapred.third.comparable.TextPair;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HighBidsByCityTest {

    private MapDriver<LongWritable, Text, TextPair, IntWritable> mapDriver;
    private ReduceDriver<TextPair, IntWritable, Text, IntWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, TextPair, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        HighBidsByCityMapper mapper = new HighBidsByCityMapper();
        HighBidsByCityReducer reducer = new HighBidsByCityReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {

        // replaced values from real entries with ordinal numbers for ease of testing;
        // kept user agent (index is 4), city id (7) and bidding price (19) normal

        String userAgentStr = "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0)";

        mapDriver.withInput(new LongWritable(), new Text("0\t1\t2\t3\t" + userAgentStr + "\t5\t6\t101\t8\t9\t10\t11\t12\t13\t14\t15\t16\t17\t18\t251\t20\t21\t22\t23\n"));
        mapDriver.withInput(new LongWritable(), new Text("0\t1\t2\t3\t" + userAgentStr + "\t5\t6\t102\t8\t9\t10\t11\t12\t13\t14\t15\t16\t17\t18\t473\t20\t21\t22\t23\n"));
        mapDriver.withInput(new LongWritable(), new Text("0\t1\t2\t3\t" + userAgentStr + "\t5\t6\t103\t8\t9\t10\t11\t12\t13\t14\t15\t16\t17\t18\t250\t20\t21\t22\t23\n"));
        mapDriver.withInput(new LongWritable(), new Text("0\t1\t2\t3\t" + userAgentStr + "\t5\t6\t104\t8\t9\t10\t11\t12\t13\t14\t15\t16\t17\t18\t358\t20\t21\t22\t23\n"));

        String os = new UserAgent(userAgentStr).getOperatingSystem().getName();
        IntWritable one = new IntWritable(1);

        mapDriver.withOutput(new TextPair("jinhua", os), one); // testing \t in dictionary
        mapDriver.withOutput(new TextPair("quzhou", os), one); // testing space in dictionary
        mapDriver.withOutput(new TextPair("unknown", os), one); // testing unknown city id

        mapDriver.withCacheFile("city.en.txt").runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> countValues = new ArrayList<>();
        countValues.add(new IntWritable(3));
        countValues.add(new IntWritable(1));

        reduceDriver.withInput(new TextPair("test city", "test OS"), countValues);
        reduceDriver.withOutput(new Text("test city"), new IntWritable(4));

        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {

        // replaced values from real entries with ordinal numbers for ease of testing;
        // kept user agent (index is 4), city id (7) and bidding price (19) normal

        String userAgentStr = "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0)";

        mapReduceDriver.withInput(new LongWritable(), new Text("0\t1\t2\t3\t" + userAgentStr + "\t5\t6\t101\t8\t9\t10\t11\t12\t13\t14\t15\t16\t17\t18\t251\t20\t21\t22\t23\n"));
        mapReduceDriver.withInput(new LongWritable(), new Text("0\t1\t2\t3\t" + userAgentStr + "\t5\t6\t102\t8\t9\t10\t11\t12\t13\t14\t15\t16\t17\t18\t473\t20\t21\t22\t23\n"));
        mapReduceDriver.withInput(new LongWritable(), new Text("0\t1\t2\t3\t" + userAgentStr + "\t5\t6\t103\t8\t9\t10\t11\t12\t13\t14\t15\t16\t17\t18\t250\t20\t21\t22\t23\n"));
        mapReduceDriver.withInput(new LongWritable(), new Text("0\t1\t2\t3\t" + userAgentStr + "\t5\t6\t104\t8\t9\t10\t11\t12\t13\t14\t15\t16\t17\t18\t358\t20\t21\t22\t23\n"));
        mapReduceDriver.withInput(new LongWritable(), new Text("0\t1\t2\t3\t" + userAgentStr + "\t5\t6\t102\t8\t9\t10\t11\t12\t13\t14\t15\t16\t17\t18\t312\t20\t21\t22\t23\n"));
        mapReduceDriver.withInput(new LongWritable(), new Text("0\t1\t2\t3\t" + userAgentStr + "\t5\t6\t103\t8\t9\t10\t11\t12\t13\t14\t15\t16\t17\t18\t521\t20\t21\t22\t23\n"));

        mapReduceDriver.withOutput(new Text("jinhua"), new IntWritable(1));
        mapReduceDriver.withOutput(new Text("quzhou"), new IntWritable(2));
        mapReduceDriver.withOutput(new Text("unknown"), new IntWritable(1));
        mapReduceDriver.withOutput(new Text("zhoushan"), new IntWritable(1));

        mapReduceDriver.withCacheFile("city.en.txt").runTest();
    }
}
