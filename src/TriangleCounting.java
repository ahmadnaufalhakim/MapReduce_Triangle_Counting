import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class TriangleCounting extends Configured implements Tool {
    public static class MyMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
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

    public static class MyReducer extends Reducer<LongWritable, LongWritable, Text, Text> {
        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            ArrayList<Long> valuesCopy = new ArrayList<Long>();
            for (LongWritable u : values) {
                valuesCopy.add(u.get());
                context.write(new Text(key.toString() + ',' + u.toString()), new Text("$"));
            }

            for (int u = 0; u < valuesCopy.size(); u++) {
                for (int w = u; w < valuesCopy.size(); w++) {
                    int compare = valuesCopy.get(u).compareTo(valuesCopy.get(w));
                    if (compare < 0) {
                        context.write(new Text(valuesCopy.get(u).toString() + ',' + valuesCopy.get(w).toString()), new Text(key.toString()));
                    }
                }
            }
        }
    }

    public int run(String[] args) throws Exception {
        Job jobOne = new Job(getConf());
        jobOne.setJobName("test-mapreduce");

        jobOne.setMapOutputKeyClass(LongWritable.class);
        jobOne.setMapOutputValueClass(LongWritable.class);

        jobOne.setOutputKeyClass(Text.class);
        jobOne.setOutputValueClass(Text.class);

        jobOne.setJarByClass(TriangleCounting.class);
        jobOne.setMapperClass(MyMapper.class);
        jobOne.setReducerClass(MyReducer.class);
        
        FileInputFormat.addInputPath(jobOne, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobOne, new Path("/user/temp/test-mapreduce"));

        int ret = jobOne.waitForCompletion(true)
            ? 0
            : 1;
        
        return ret;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TriangleCounting(), args);
        System.exit(res);
    }
}