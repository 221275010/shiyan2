import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
//./hadoop jar /home/njucs/shiyan2_2/target/shiyan2_2-1.0-SNAPSHOT.jar WeeklyBalanceAnalysis /input /output_2
public class WeeklyBalanceAnalysis {

    public static class WeeklyMapper extends Mapper<LongWritable, Text, Text, BalanceWritable> {
        private BalanceWritable balanceWritable = new BalanceWritable();
        private Text weekKey = new Text();
        private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length != 2)
                return;

            String[] amounts = fields[1].split(",");
            if (amounts.length != 2)
                return;

            BigInteger inflow = new BigInteger(amounts[0]);
            BigInteger outflow = new BigInteger(amounts[1]);

            // int lineIndex = (int) (key.get() + 1);
            // int dayIndex = (lineIndex - 1) % 7; // 0到6表示一周中的天
            // String weekDay = getWeekDay(dayIndex);
            String dateString = fields[0];
            Calendar calendar = Calendar.getInstance();
            try {
                calendar.setTime(dateFormat.parse(dateString));
            } catch (ParseException e) {
                e.printStackTrace();
            }
            int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK) - 1; // 转换为0（周日）到6（周六）

            String weekDay = getWeekDay(dayOfWeek);

            weekKey.set(weekDay);
            balanceWritable.set(inflow, outflow);
            context.write(weekKey, balanceWritable);
        }

        private String getWeekDay(int index) {
            switch (index) {
                case 0: return "Sunday";
                case 1: return "Monday";
                case 2: return "Tuesday";
                case 3: return "Wednesday";
                case 4: return "Thursday";
                case 5: return "Friday";
                case 6: return "Saturday";
                default: return "Unknown";
            }
        }
    }

    public static class WeeklyReducer extends Reducer<Text, BalanceWritable, Text, Text> {
        private Text result = new Text();
        private TreeMap<BigInteger, String> sortedResults = new TreeMap<>(Comparator.reverseOrder());

        @Override
        protected void reduce(Text key, Iterable<BalanceWritable> values, Context context) throws IOException, InterruptedException {
            BigInteger totalInflow = BigInteger.ZERO;
            BigInteger totalOutflow = BigInteger.ZERO;
            int count = 0;

            for (BalanceWritable val : values) {
                totalInflow = totalInflow.add(val.getInflow());
                totalOutflow = totalOutflow.add(val.getOutflow());
                count++;
            }

            BigInteger avgInflow = totalInflow.divide(BigInteger.valueOf(61));
            BigInteger avgOutflow = totalOutflow.divide(BigInteger.valueOf(61));

            // 使用 TreeMap 存储结果，按资金流入量排序
            sortedResults.put(avgInflow, key.toString() + "," + avgOutflow);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 输出排序后的结果
            for (Map.Entry<BigInteger, String> entry : sortedResults.entrySet()) {
                String[] values = entry.getValue().split(","); // 分割星期和流入、流出
                String day = values[0].trim(); // 获取星期
                String out_num = values[1].trim();
                result.set(entry.getKey().toString() +","+ out_num);
                context.write(new Text(day), result);
                
            }
        }
    }

    public static class BalanceWritable implements Writable {
        private BigInteger inflow;
        private BigInteger outflow;

        public BalanceWritable() {
            this.inflow = BigInteger.ZERO;
            this.outflow = BigInteger.ZERO;
        }

        public void set(BigInteger inflow, BigInteger outflow) {
            this.inflow = inflow;
            this.outflow = outflow;
        }

        public BigInteger getInflow() {
            return inflow;
        }

        public BigInteger getOutflow() {
            return outflow;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(inflow.toString());
            out.writeUTF(outflow.toString());
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            inflow = new BigInteger(in.readUTF());
            outflow = new BigInteger(in.readUTF());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Weekly Balance Analysis");
        job.setJarByClass(WeeklyBalanceAnalysis.class);
        job.setMapperClass(WeeklyMapper.class);
        job.setReducerClass(WeeklyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BalanceWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}