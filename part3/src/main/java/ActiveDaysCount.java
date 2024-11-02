import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ActiveDaysCount {

    public static class ActiveDaysMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private Text userId = new Text();
        private LongWritable one = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            // if (fields.length < 13) return; // Ensure correct number of fields

            String userIdField = fields[0];
            BigInteger directPurchaseAmt = new BigInteger(fields[5]);
            BigInteger totalRedeemAmt = new BigInteger(fields[8]);

            userId.set(userIdField);
            if (directPurchaseAmt.compareTo(BigInteger.ZERO) > 0 || totalRedeemAmt.compareTo(BigInteger.ZERO) > 0) {
                context.write(userId, one);
            } else {
                context.write(userId, new LongWritable(0)); // Count as active day even if amounts are zero
            }
                }
            }

    public static class ActiveDaysReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable result = new LongWritable();
        private Map<Long, List<Text>> activeDaysMap = new TreeMap<>(Comparator.reverseOrder());
    
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long activeDays = 0;
            for (LongWritable val : values) {
                activeDays += val.get();
            }
            result.set(activeDays);
            
            // Store in Map: key is active days, value is a list of userIds
            activeDaysMap.computeIfAbsent(activeDays, k -> new ArrayList<>()).add(new Text(key));
        }
    
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Output the sorted results
            for (Map.Entry<Long, List<Text>> entry : activeDaysMap.entrySet()) {
                for (Text userId : entry.getValue()) {
                    context.write(userId, new LongWritable(entry.getKey()));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Active Days Count");
        job.setJarByClass(ActiveDaysCount.class);
        job.setMapperClass(ActiveDaysMapper.class);
        job.setReducerClass(ActiveDaysReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}