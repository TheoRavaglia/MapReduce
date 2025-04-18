package com.company.mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {

    // 1. Number of transactions involving Brazil
    public static class BrazilMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final Text BRAZIL = new Text("Brazil");
        private final IntWritable one = new IntWritable(1);
        private boolean isHeader = true;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (isHeader) {
                isHeader = false;
                return;
            }
            String[] fields = line.split(";");
            if (fields.length == 10 && fields[0] != null && fields[0].equalsIgnoreCase("Brazil")) {
                context.write(BRAZIL, one);
            }
        }
    }

    public static class BrazilReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    // 2. Number of transactions per year
    public static class YearMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final Text yearKey = new Text();
        private final IntWritable one = new IntWritable(1);
        private boolean isHeader = true;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (isHeader) {
                isHeader = false;
                return;
            }
            String[] fields = line.split(";");
            if (fields.length == 10 && fields[1] != null && !fields[1].isEmpty()) {
                yearKey.set(fields[1]);
                context.write(yearKey, one);
            }
        }
    }

    public static class YearReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    // 3. Number of transactions per category
    public static class CategoryMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final Text categoryKey = new Text();
        private final IntWritable one = new IntWritable(1);
        private boolean isHeader = true;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (isHeader) {
                isHeader = false;
                return;
            }
            String[] fields = line.split(";");
            if (fields.length == 10 && fields[9] != null && !fields[9].isEmpty()) {
                categoryKey.set(fields[9]);
                context.write(categoryKey, one);
            }
        }
    }

    public static class CategoryReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    // Unified main method to choose job
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: Main <job_id> <input path> <output path>");
            System.err.println("job_id = 1 (Brazil), 2 (Year), 3 (Category)");
            System.exit(-1);
        }

        String jobId = args[0];
        String input = args[1];
        String output = args[2];

        Configuration conf = new Configuration();
        Job job = null;

        switch (jobId) {
            case "1":
                job = Job.getInstance(conf, "Brazil Transaction Count");
                job.setJarByClass(Main.class);
                job.setMapperClass(BrazilMapper.class);
                job.setReducerClass(BrazilReducer.class);
                break;

            case "2":
                job = Job.getInstance(conf, "Transactions Per Year");
                job.setJarByClass(Main.class);
                job.setMapperClass(YearMapper.class);
                job.setReducerClass(YearReducer.class);
                break;

            case "3":
                job = Job.getInstance(conf, "Transactions Per Category");
                job.setJarByClass(Main.class);
                job.setMapperClass(CategoryMapper.class);
                job.setReducerClass(CategoryReducer.class);
                break;

            default:
                System.err.println("Invalid job_id. Use 1, 2 or 3.");
                System.exit(-1);
        }

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
