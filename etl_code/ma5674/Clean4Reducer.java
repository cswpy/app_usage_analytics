import java.io.IOException;
import java.io.*;
import java.util.*;
import org.apache.hadoop.io.IntWritable; import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Clean4Reducer
        extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String outkey = key.toString() + "~";
        
        String[] prev = new String[5];


        String current = "";
        int n = 0;
        for (Text value : values) 
        {
            
            if (n < 5)
            {
                prev[n] = value.toString();
                n++;

                if (n == 5)
                {
                    current = prev[0];
                    for (int i = 1; i < 5; i++) 
                    {
                        current = current + "," + prev[i];
                    }
                    context.write(new Text(outkey), new Text(current));
                }
            }
            else if (n == 5)
            {
                prev[0] = prev[1];
                prev[1] = prev[2];
                prev[2] = prev[3];
                prev[3] = prev[4];
                prev[4] = value.toString();

                current = prev[0];
                for (int i = 1; i < 5; i++) 
                {
                    current = current + "," + prev[i];
                }
                context.write(new Text(outkey), new Text(current));
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


