package partition;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * @author sreeveni
 *
 */
public class PartitionReducer extends
		Reducer<Text, Text, NullWritable, NullWritable> {

	MultipleOutputs<NullWritable, Text> mos;
	NullWritable out = NullWritable.get();

	@Override
	protected void setup(Context context) {
		mos = new MultipleOutputs(context);
	}

	public void reduce(Text key, Iterable<Text> values, Context context) {
		for (Text value : values) {
			try {
				mos.write(out, value, key.toString());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	protected void cleanup(Context context) {
		try {
			mos.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
