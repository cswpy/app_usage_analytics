import java.io.IOException;
import org.apache.hadoop.io.IntWritable; import org.apache.hadoop.io.LongWritable; import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class Clean4Mapper
    extends Mapper<LongWritable, Text, Text, Text> {

  private static final int MISSING = 9999;

  @Override
  public void map(LongWritable key, Text value, Context context)
     throws IOException, InterruptedException {
      
    String line = value.toString();
    String[] arr = line.trim().split("~");
    // Text val = new Text(key + arr[1]);

    context.write(new Text(arr[0]),new Text(arr[1]));
  }

}


// public class Clean2Mapper
//     extends Mapper<LongWritable, Text, Text, Long> {

//   private static final int MISSING = 9999;

//   @Override
//   public void map(LongWritable key, Text value, Context context)
//      throws IOException, InterruptedException {

//     String line = value.toString();
//     // convert the input line to arrays by splitting them
//     String[] arr = line.split("\t");
//     String outputKey;
//     long keyout = key.get();

//     // skip the first line of the input, as it's just headers
//     if (key.get()==0)
//     {
//       return;
//     }
//     else
//     {
//         outputKey = arr[0]+ "," + arr[3];
//         context.write(new Text(outputKey), keyout);      
//       }
//     }
//   }



