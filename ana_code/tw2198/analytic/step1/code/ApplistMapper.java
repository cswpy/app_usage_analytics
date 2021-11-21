import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ApplistMapper
    extends Mapper<LongWritable, Text, Text, IntWritable>{
    
    public void map(LongWritable key, Text value, Context context) 
        throws IOException, InterruptedException {
        
        String line = value.toString();
        Matcher m = Pattern.compile("('(.*?)')|(\"\"(.*?)\"\")").matcher(line);

        List<String> appList = new ArrayList<>();
        while(m.find()){
            String rawList = m.group().trim();
            rawList.replace('\'', '\0');
            rawList.replace('\"', '\0');
            if(!rawList.contains("?") && 
                !rawList.contains(":") &&
                !rawList.contains("google_play")){
                appList.add(rawList);
            }
        }
       
        for(int i = 0; i < appList.size() - 3; i++){
            String temp = appList.get(i) + " " +appList.get(i + 1) + " " +appList.get(i + 2) + '~';
            context.write(new Text(temp), new IntWritable(1));
        }
    }
}