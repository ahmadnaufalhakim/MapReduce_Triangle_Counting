import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class TriangleCounting extends Configured implements Tool {
    static final String firstmapreducepath = "/home/ubuntu/MapReduce_Triangle_Counting/temp/output-first-mapreduce";
    static final String secondmapreducepath = "/home/ubuntu/MapReduce_Triangle_Counting/temp/output-second-mapreduce";
    public static class FirstMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
        public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
            String[] pair = text.toString().split("\\s+");
            if (pair.length > 1) {
                long u = Long.parseLong(pair[0]);
                long v = Long.parseLong(pair[1]);

                if (u < v) {
                    context.write(new LongWritable(u), new LongWritable(v));
                } else {
                    context.write(new LongWritable(v), new LongWritable(u));
                }
            }
        }
    }

    public static class FirstReducer extends Reducer<LongWritable, LongWritable, Text, Text> {
        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            ArrayList<Long> valuesCopy = new ArrayList<Long>();
            // Format > key : value -> v u
            for (LongWritable u : values) {
                valuesCopy.add(u.get());
                context.write(new Text(key.toString()), new Text(u.toString()));
            }
            // Format > key : value -> v u,w
            for (int u = 0; u < valuesCopy.size(); u++) {
                for (int w = u; w < valuesCopy.size(); w++) {
                    int compare = valuesCopy.get(u).compareTo(valuesCopy.get(w));
                    if (compare > 0) {
                        // Format key value -> 1 2,3
                        context.write(new Text(key.toString()), new Text(valuesCopy.get(u).toString() + "," + valuesCopy.get(w).toString()));
                    }
                }
            }
        }
    }

    public static class SecondMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
            String[] pair = text.toString().split("\\s+");
            if (pair.length > 1) {
                long u = Long.parseLong(pair[0]);

                String[] nextpair = pair[1].split(",");
                if (nextpair.length > 1){
                    long v = Long.parseLong(nextpair[0]);
                    long w = Long.parseLong(nextpair[1]);
                    // Key : v,w value : u
                    context.write(new Text(Long.toString(v)+ ','+ Long.toString(w)), new Text(Long.toString(u)));
                }
                else {
                    long v = Long.parseLong(pair[1]);

                    if (u < v) {
                        // Key : u,v value : $
                        context.write(new Text(Long.toString(u)+ ','+Long.toString(v)), new Text("$"));
                    } else {
                        // Key : v,u value : $
                        context.write(new Text(Long.toString(v)+ ','+Long.toString(u)), new Text("$"));
                    }
                }
            }
        }
    }

    public static class SecondReducer extends Reducer<Text, Text, LongWritable, LongWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            LinkedHashSet<String> valueSet = new LinkedHashSet<String>();
            for (Text value : values) {
                valueSet.add(value.toString());
            }
            long count_sum = 0;
            boolean valid = false;
            for (String value : valueSet) {
                if (!value.equals("$")){
                    ++count_sum;
                } else {
                    valid = true;
                }
            }
            if (valid) {
                // 0 sum
                context.write(new LongWritable(0), new LongWritable(count_sum));
            }
        }
    }

    public static class ThirdMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
        public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
            String[] pair = text.toString().split("\\s+");
            if (pair.length > 1) {
                context.write(new LongWritable(0), new LongWritable(Long.parseLong(pair[1])));
            }
        }
    }

    public static class ThirdReducer extends Reducer<LongWritable, LongWritable, Text, LongWritable> {
        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(new Text("Result"), new LongWritable(sum));
        }
    }

    public int run(String[] args) throws Exception {
        // Path
        

        // MapReduce 1
        Job jobFirst = new Job(getConf());
        jobFirst.setJobName("mapreduce-1");

        jobFirst.setMapOutputKeyClass(LongWritable.class);
        jobFirst.setMapOutputValueClass(LongWritable.class);

        jobFirst.setOutputKeyClass(Text.class);
        jobFirst.setOutputValueClass(Text.class);

        jobFirst.setJarByClass(TriangleCounting.class);
        jobFirst.setMapperClass(FirstMapper.class);
        jobFirst.setReducerClass(FirstReducer.class);
        
        FileInputFormat.addInputPath(jobFirst, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobFirst, new Path(firstmapreducepath));

        // MapReduce 2
        Job jobSecond = new Job(getConf());
        jobSecond.setJobName("mapreduce-2");

        jobSecond.setMapOutputKeyClass(Text.class);
        jobSecond.setMapOutputValueClass(Text.class);

        jobSecond.setOutputKeyClass(LongWritable.class);
        jobSecond.setOutputValueClass(LongWritable.class);

        jobSecond.setJarByClass(TriangleCounting.class);
        jobSecond.setMapperClass(SecondMapper.class);
        jobSecond.setReducerClass(SecondReducer.class);

        FileInputFormat.addInputPath(jobSecond, new Path(args[0]));
        FileInputFormat.addInputPath(jobSecond, new Path(firstmapreducepath));
        FileOutputFormat.setOutputPath(jobSecond, new Path(secondmapreducepath));

        // MapReduce 3
        Job jobThird = new Job(getConf());
        jobThird.setJobName("mapreduce-2");

        jobThird.setMapOutputKeyClass(LongWritable.class);
        jobThird.setMapOutputValueClass(LongWritable.class);

        jobThird.setOutputKeyClass(Text.class);
        jobThird.setOutputValueClass(LongWritable.class);

        jobThird.setJarByClass(TriangleCounting.class);
        jobThird.setMapperClass(ThirdMapper.class);
        jobThird.setReducerClass(ThirdReducer.class);

        FileInputFormat.addInputPath(jobThird, new Path(secondmapreducepath));
        FileOutputFormat.setOutputPath(jobThird, new Path(args[1]));

        int ret = jobFirst.waitForCompletion(true) ? 0 : 1;
        if (ret == 0) {
            ret = jobSecond.waitForCompletion(true) ? 0 : 1;
        }
        if (ret == 0) {
            ret = jobThird.waitForCompletion(true) ? 0 : 1;
        }
        return ret;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TriangleCounting(), args);
        System.exit(res);
    }
}