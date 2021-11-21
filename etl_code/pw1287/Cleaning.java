import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Cleaning{

public static class CleaningMapper
  extends Mapper<LongWritable, Text, IntWritable, Text> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    if(key.get() > 0){ // Meaning the row is not the header
      String line = value.toString();
      String[] values = line.split("\t");
      String[] part1 = Arrays.copyOfRange(values, 0, 3);
      String[] part2 = Arrays.copyOfRange(values, 9, 11);
      String trimmedString = String.join(",", part1);
      trimmedString += ",";
      trimmedString += String.join(",", part2);
      context.write(new IntWritable(1), new Text(trimmedString)); // Output a pair line # as key
    }
  }
}

public static class CleaningReducer
  extends Reducer<IntWritable, Text, Text, Text> {
  
  @Override
  public void reduce(IntWritable key, Iterable<Text> values,
      Context context)
      throws IOException, InterruptedException {
        for (Text value : values) {
          String[] val = value.toString().split(",");
          context.write(new Text(val[0]), new Text(String.join(",", Arrays.copyOfRange(val, 1, 5))));
        }
  }
}

public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: Cleaning <input path> <output path>");
      System.exit(-1);
    }
    
    Job job = new Job();
    job.setJarByClass(Cleaning.class);
    job.setJobName("Cleaning");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapOutputKeyClass(IntWritable.class);     
    job.setMapOutputValueClass(Text.class);    

    job.setMapperClass(CleaningMapper.class);
    job.setReducerClass(CleaningReducer.class);

    job.setNumReduceTasks(1);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
