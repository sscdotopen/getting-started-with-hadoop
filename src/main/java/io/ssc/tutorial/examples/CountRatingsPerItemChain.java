package io.ssc.tutorial.examples;

import io.ssc.tutorial.SimpleMapReduceChain;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

public class CountRatingsPerItemChain extends SimpleMapReduceChain {

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new CountRatingsPerItemChain(), args);
  }

  @Override
  public int run(String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    if (!parsedArgs.containsKey("--input") || !parsedArgs.containsKey("--output")) {
      System.err.println("Specify --input and --output pathes!");
      return -1;
    }

    Path input = new Path(parsedArgs.get("--input"));
    Path output = new Path(parsedArgs.get("--output"));

    Job countRatings = prepareJob(input, output,
        TextInputFormat.class,
        SingleRatingMapper.class, LongWritable.class, IntWritable.class,
        SumRatingsReducer.class, LongWritable.class, IntWritable.class,
        TextOutputFormat.class);
    countRatings.setCombinerClass(SumRatingsReducer.class);

    countRatings.waitForCompletion(true);

    return 0;
  }

  static class SingleRatingMapper extends Mapper<LongWritable,Text,LongWritable,IntWritable> {

    private static final Pattern DELIMITER = Pattern.compile("[\t,]");

    @Override
    protected void map(LongWritable key, Text line, Context ctx) throws IOException, InterruptedException {
      long itemID = Long.parseLong(DELIMITER.split(line.toString())[1]);
      ctx.write(new LongWritable(itemID), new IntWritable(1));
    }
  }

  static class SumRatingsReducer extends Reducer<LongWritable,IntWritable,LongWritable,IntWritable> {
    @Override
    protected void reduce(LongWritable itemID, Iterable<IntWritable> counts, Context ctx)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable count : counts) {
        sum += count.get();
      }
      ctx.write(itemID, new IntWritable(sum));
    }
  }

}
