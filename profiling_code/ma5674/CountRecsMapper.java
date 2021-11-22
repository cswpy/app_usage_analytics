import java.io.IOException;
import org.apache.hadoop.io.IntWritable; import org.apache.hadoop.io.LongWritable; import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class CountRecsMapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {

  private static final int MISSING = 9999;

  @Override
  public void map(LongWritable key, Text value, Context context)
     throws IOException, InterruptedException {

      // here, we check each time a record appears and the map function uses it, we add a 1 for the instance to the map output
      
    String line = value.toString();
    int total;

    // skip the first record of the input, as it's just headers
    if (key.get()==0)
    {
      return;
    }
    else
    {
      context.write(new Text("records"), new IntWritable(1));      
    }
  }

}
