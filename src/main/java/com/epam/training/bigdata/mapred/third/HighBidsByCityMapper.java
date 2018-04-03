package com.epam.training.bigdata.mapred.third;

import com.epam.training.bigdata.mapred.third.comparable.TextPair;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Takes a log entry and maps it to <<city, OS>, 1> pair if bid price is greater than 250.
 */
public class HighBidsByCityMapper extends Mapper<LongWritable, Text, TextPair, IntWritable> {

    private static final int MIN_BID_THRESHOLD = 250;
    private static final IntWritable ONE = new IntWritable(1);

    private Text cityText = new Text();
    private Text osText = new Text();
    private TextPair cityAndOS = new TextPair();
    private Map<String, String> cities = new HashMap<>();

    @Override
    public void setup(Context context) throws IOException {
        URI[] cacheFiles = context.getCacheFiles();
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path path = new Path(cacheFiles[0].toString());
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts;
                if (line.contains("\t")) {
                    parts = line.split("\t");
                } else {
                    parts = line.split(" ");
                }

                cities.put(parts[0], parts[1]);
            }
        }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");
        String cityName = cities.get(parts[7]);
        if (cityName == null) {
            cityName = "unknown";
            context.getCounter("Wrong Input Data Counters", "Number of city IDs not found in dictionary").increment(1);
        }

        int bidPrice = Integer.parseInt(parts[19]);
        if (bidPrice > MIN_BID_THRESHOLD) {
            UserAgent userAgent = new UserAgent(parts[4]);
            String os = userAgent.getOperatingSystem().getName();

            cityText.set(cityName);
            osText.set(os);

            cityAndOS.set(cityText, osText);
            context.write(cityAndOS, ONE);
        }
    }
}
