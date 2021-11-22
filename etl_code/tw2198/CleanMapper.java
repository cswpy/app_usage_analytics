import java.util.*;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper
    extends Mapper<LongWritable, Text, Text, Text>{
    
    public void map(LongWritable key, Text value, Context context) 
        throws IOException, InterruptedException {
        
        String line = value.toString();
        String[] cells = line.split("~");
         
        // Schema, size = 6
        // 0:id 1:timestamp 2:UserId 3:Query 4:App 5:AppUsages

        try {
            if(cells.length == 6){
               if(!(cells[5].contains("{'Duration': {}}") && 
                    !(cells[5].contains("???")))){
                    context.write(new Text(cells[4] + '~'), new Text(cells[5]));
               } 
            }
        } catch (Exception e) {}
    }
}