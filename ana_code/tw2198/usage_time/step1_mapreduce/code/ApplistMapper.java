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
        Matcher m = Pattern.compile("(('(.*?)')|(\"\"(.*?)\"\"))(:{1})\\s\\d+").matcher(line);

        List<String> appList = new ArrayList<>();
        while(m.find()){
            String rawList = m.group().trim();
            if(!rawList.contains("?") && 
                !rawList.contains("google_play")){
                appList.add(rawList);
            }
        }
       
        for(String s: appList){
            String[] token = s.split(":");
            if(token.length == 2){
                String appName = token[0];
                appName = appName.trim();
                appName = appName.replace('\'', '\u0000');
                appName = appName.replace('\"', '\u0000');

                String timelength = token[1];
                timelength = timelength.trim();
                int timeInt = Integer.parseInt(timelength);

                if(timeInt != 0){
                    context.write(new Text(appName + "~"), new IntWritable(timeInt));
                }
            }else if(token.length == 3){
                String appName = token[1];
                appName = appName.trim();
                appName = appName.replace('\'', '\u0000');
                appName = appName.replace('\"', '\u0000');
                appName = appName.replace('{', '\u0000');
                appName = appName.replace('}', '\u0000');

                String timelength = token[2];
                timelength = timelength.trim();
                int timeInt = Integer.parseInt(timelength);

                if(timeInt != 0){
                    context.write(new Text(appName), new IntWritable(timeInt));
                }
            }
        }
    }
}