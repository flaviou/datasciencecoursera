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

public class MeanDelayOriginDestination {

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
            String lsOriginAirportID = "";
            String lsDestinationAirportID = "";
            double liArrDelayMinutes = 0;
            double liCancelled = 0;
            double liDiverted = 0;

            int position = 0;

            for (String nextToken : tokens) {
                switch (position) {
                    case OriginAirportID: 
                      lsOriginAirportID = nextToken;
                      break;
                    case DestinationAirportID: 
                      lsDestinationAirportID = nextToken;
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

            if ((liCancelled == 0) && (liDiverted ==0)) {
              context.write(new Text(lsOriginAirportID + "-" + lsDestinationAirportID), new IntWritable((int) liArrDelayMinutes));
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
                sum += delay;
                count ++;
            }
            float mean = (float) sum / count;
            context.write(key, new FloatWritable(mean));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job jobA = Job.getInstance(conf, "delaysum");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(ArrivalMap.class);
        jobA.setReducerClass(ArrivalReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, new Path(args[1]));

        jobA.setJarByClass(MeanDelayOriginDestination.class);

        System.exit(jobA.waitForCompletion(true) ? 0 : 1);

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
