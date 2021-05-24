
package Access;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ALReducer extends Reducer <Text,IntWritable,Text,IntWritable> {
	
	public void reduce (Text t, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
		
		int sum=0;
	
		//find sum of occurrences
		for( IntWritable value:values) {
			sum+=value.get();
		}
		
		
		con.write(t, new IntWritable(sum));
	}
}