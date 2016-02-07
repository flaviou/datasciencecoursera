import java.io.IOException;
import java.lang.Integer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TopArrivalPerformanceAirlines {

    public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }

        public TextArrayWritable(String[] strings) {
            super(Text.class);
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            set(texts);
        }
    }

    public static class ArrivalMap extends Mapper<Object, Text, Text, IntWritable> {
        final int AirlineID = 7;
        final int OriginAirportID = 11;
        final int DestinationAirportID = 17;
        final int ArrDelayMinutes = 37;
        final int ArrDel15 = 38;
        final int Cancelled = 41;
        final int Diverted = 43;

        public static boolean isNumeric(String str)
        {
          return str.matches("-?\\d+(\\.\\d+)?");  //match a number with optional '-' and decimal.
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
            String lsAirlineID = "";
            double liArrDelayMinutes = 0;
            double liArrDel15 = 0;
            double liCancelled = 0;
            double liDiverted = 0;

            int position = 0;

            for (String nextToken : tokens) {
                switch (position) {
                    case AirlineID: 
                      lsAirlineID = nextToken;
                      break;
                    case ArrDelayMinutes:
                      if ((nextToken != null) && !nextToken.isEmpty()) {
                        if (isNumeric(nextToken)) {
                          liArrDelayMinutes = Double.parseDouble(nextToken);
                        }
                      } else {
                        liArrDelayMinutes = 0;
                      }
                      break;
                    case Cancelled:
                      if (isNumeric(nextToken)) {
                        liCancelled = Double.parseDouble(nextToken);
                      }
                      break;
                    case Diverted:
                      if (isNumeric(nextToken)) {
                        liDiverted = Double.parseDouble(nextToken);
                      }
                      break;
                }
                position++;
            }
            if ((liCancelled == 0) && (liDiverted ==0) ) {
              context.write(new Text(lsAirlineID), new IntWritable((int) liArrDelayMinutes));
            }
        }
    }

    public static class ArrivalReduce extends Reducer<Text, IntWritable, Text, FloatWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            int delay = 0;
            for (IntWritable val : values) {
                delay = val.get();
                if (delay > 15) {
                  sum += delay;
                }
                count ++;
            }
            float avg = (float) sum / count;
            context.write(key, new FloatWritable(avg));
        }
    }

    public static class DelayMap extends Mapper<Text, Text, NullWritable, TextArrayWritable> {
        private TreeSet<Pair<Float, String>> avgDelayMap = new TreeSet<Pair<Float, String>>();

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Float avg = Float.parseFloat(value.toString());
            String word = key.toString();
            avgDelayMap.add(new Pair<Float, String>(avg, word));
            if (avgDelayMap.size() > 10) {
                avgDelayMap.remove(avgDelayMap.last());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Pair<Float, String> item : avgDelayMap) {
                String[] strings = {item.second, item.first.toString()};
                TextArrayWritable val = new TextArrayWritable(strings);
                context.write(NullWritable.get(), val);
            }
        }
    }

    public static class DelayReduce extends Reducer<NullWritable, TextArrayWritable, Text, FloatWritable> {
        private TreeSet<Pair<Float, String>> avgDelayMap = new TreeSet<Pair<Float, String>>();

        @Override
        public void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
            for (TextArrayWritable val: values) {
                Text[] pair= (Text[]) val.toArray();

                String word = pair[0].toString();
                Float avg = Float.parseFloat(pair[1].toString());

                avgDelayMap.add(new Pair<Float, String>(avg, word));
                if (avgDelayMap.size() > 10) {
                    avgDelayMap.remove(avgDelayMap.last());
                }
            }
            for (Pair<Float, String> item: avgDelayMap) {
                Text word = new Text(item.second);
                FloatWritable value = new FloatWritable(item.first);
                context.write(word, value);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/w1/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "delaysum");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(ArrivalMap.class);
        jobA.setReducerClass(ArrivalReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);
        jobA.setJarByClass(TopArrivalPerformanceAirlines.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Best Airline");
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(FloatWritable.class);

        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(TextArrayWritable.class);

        jobB.setMapperClass(DelayMap.class);
        jobB.setReducerClass(DelayReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(TopArrivalPerformanceAirlines.class);
        System.exit(jobB.waitForCompletion(true) ? 0 : 1);

    }
}

class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}
