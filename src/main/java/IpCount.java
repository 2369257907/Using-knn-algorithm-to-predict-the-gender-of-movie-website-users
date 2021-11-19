
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IpCount {
    public static class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text outKey = new Text();
        private IntWritable outValue = new IntWritable(1);//可以替换为NullWritable

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 获取一行内容的字符串类型
            String line = value.toString();

            String pattern = "((25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))";

            Pattern r = Pattern.compile(pattern);
            Matcher m = r.matcher(line);
            while (m.find()) {
                System.out.println(m.group(0));
                outKey.set(m.group(0));
                context.write(outKey, outValue);

            }

        }

        public static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
            private IntWritable outValue = new IntWritable(); // 放reduce中调用一次reduce就会创建一个对象，所以做属性

            @Override
            protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                if (key.toString().equals("")) {
                    return;
                }
                int count = 0;
                for (IntWritable value : values) {
                    count += value.get();
                }
                outValue.set(count);
                context.write(key, outValue);
            }
        }

        public static void main(String[] args) throws Exception {
            BasicConfigurator.configure();
            if (args == null || args.length < 2) {
                System.out.println("参数个数不正确，必须输入两个路径参数!");
                return;
            }
            Configuration conf = new Configuration();
            // 获取流水线作业对象
            Job job = Job.getInstance(conf);
            // 4 4个输出类型
            job.setMapOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            // 3 3个环节类型
            job.setJarByClass(IpCount.class);
            job.setMapperClass(WCMapper.class);
            job.setReducerClass(WCReducer.class);
            // 2 2个位置：数据计算的输入和输出位置
            FileInputFormat.setInputPaths(job, new Path(args[0]));
            Path outPath = new Path(args[1]);
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(outPath)) {  // 避免因为输出路径存在而产生错误
                fs.delete(outPath, true);
            }
            FileOutputFormat.setOutputPath(job, outPath);
            // 1 1次提交
            boolean result = job.waitForCompletion(true);
            System.exit(result ? 0 : 1);// 0正常终止 1强制终止  可有可无，一条优化语句
        }
    }
}
