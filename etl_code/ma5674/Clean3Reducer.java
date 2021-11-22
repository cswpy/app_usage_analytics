import java.io.IOException;
import org.apache.hadoop.io.IntWritable; import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Clean3Reducer
        extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String outkey = key.toString() + "~";

        String prev = "";
        String current;
        for (Text value : values) 
        {
            current = value.toString();
            if (!(current.equals(prev)))
            {
                context.write(new Text(outkey), value);
                prev = current;
            }
            
        }
               
        // for (Text value : values) 
        // {
        //     context.write(new Text(outkey), value);
        // }

        // for (int i = 0; i < n; i++)
        // {
        //     context.write(new Text(outkey), values[i]);
        //     int j = 0;
        //     while (values[i+j] == values[i])
        //     {
        //         j++;
        //     }
        //     i = j;
        // }
        // context.write(new Text(outkey), output);
        // // String output = values.toString();
        // String output = "";

        // for (Text value : values) 
        // {
        //     output += value.toString();
        // }
        // context.write(new Text(key), output); 
    }
}

// public class Clean2Reducer 
//         extends Reducer<Text, Long, Text, Long> {
//     // @Override
//     public void reduce(Text key, Iterable<Long> values, Context context)
//             throws IOException, InterruptedException {

//         String line = key.toString();
//         String[] arr = line.split(",");
//         String outputKey = arr[0] + '\t' + arr[1];

//         // Long value = 0; 
//         for (Long value : values) {
//             context.write(new Text(outputKey), value);        
//         }

//         // context.write(new Text(outputKey), values); 
//     }
// }
