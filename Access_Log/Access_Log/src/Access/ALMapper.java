
package Access;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ALMapper extends Mapper <LongWritable,Text,Text,IntWritable> {
	
	public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
		
		String [ ] Log = value.toString().split("-");
	
		con.write(new Text(Log[0]),new IntWritable(1));
	}
}


