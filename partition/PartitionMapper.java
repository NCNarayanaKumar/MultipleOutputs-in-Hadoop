package partition;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * @author sreeveni
 * 9 Dec 2015
 */
public class PartitionMapper extends Mapper<LongWritable, Text, Text, Text> {

	Text keyEmit = new Text();
	Text valEmit = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) {

		Configuration conf = context.getConfiguration();
		String delim = conf.get("delimiter");
		String line = value.toString();
		
		String[] record = line.split(delim);
		keyEmit.set(record[3]);
			try {
				context.write(keyEmit, value);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
		}
	}

}
