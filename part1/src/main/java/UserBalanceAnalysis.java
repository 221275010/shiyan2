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

public class UserBalanceAnalysis {

    public static class BalanceMapper extends Mapper<LongWritable, Text, Text, BalanceWritable> {
        private Text dateKey = new Text();
        private BalanceWritable balanceWritable = new BalanceWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length < 10) return; // Check for valid row

            String reportDate = fields[1]; // report_date
            BigInteger totalPurchaseAmt = parseBigInteger(fields[4]); // total_purchase_amt
            BigInteger totalRedeemAmt = parseBigInteger(fields[8]); // total_redeem_amt

            dateKey.set(reportDate);
            balanceWritable.set(totalPurchaseAmt, totalRedeemAmt);
            context.write(dateKey, balanceWritable);
        }

        private BigInteger parseBigInteger(String str) {
            try {
                return new BigInteger(str);
            } catch (NumberFormatException e) {
                return BigInteger.ZERO; // Handle missing or invalid values
            }
        }
    }

    public static class BalanceReducer extends Reducer<Text, BalanceWritable, Text, Text> {
        private Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<BalanceWritable> values, Context context) throws IOException, InterruptedException {
            BigInteger totalInflow = BigInteger.ZERO;
            BigInteger totalOutflow = BigInteger.ZERO;

            for (BalanceWritable val : values) {
                totalInflow = totalInflow.add(val.getInflow());
                totalOutflow = totalOutflow.add(val.getOutflow());
            }

            result.set(totalInflow.toString() + "," + totalOutflow.toString());
            context.write(key, result);
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
        Job job = Job.getInstance(conf, "User Balance Analysis");
        job.setJarByClass(UserBalanceAnalysis.class);
        job.setMapperClass(BalanceMapper.class);
        job.setReducerClass(BalanceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BalanceWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}