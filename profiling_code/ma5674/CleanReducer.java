import java.io.IOException;
import org.apache.hadoop.io.IntWritable; import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CleanReducer
        extends Reducer<Text, Text, Text, String> {
    // @Override
    public void reduce(Text key, Text values, Context context)
            throws IOException, InterruptedException {

        String output = values.toString();

        // for (TextWritable value : values) 
        // {
        //     output += toString(value.get());
        // }
        context.write(new Text(key), output); 
    }
}