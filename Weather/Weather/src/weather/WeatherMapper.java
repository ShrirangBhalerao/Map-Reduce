package weather;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WeatherMapper extends Mapper <LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

		//get year
		String year=value.toString().substring(15,19);
		//get temperature
		//int temperature=Integer.parseInt(value.toString().substring(87,91));
		
		int temperature;
		String line =value.toString();
		if(line.charAt(87)=='+') {
			temperature=Integer.parseInt(line.substring(88,92));
		}
		else {
			temperature=Integer.parseInt(line.substring(87,92));
		}
		
		//clean the data and write to context object
		if((temperature)!=9999) {
			con.write(new Text(year), new IntWritable(temperature));
		}
	}
}