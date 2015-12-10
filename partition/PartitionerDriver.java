package partition;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author sreeveni
 *
 */
public class PartitionerDriver extends Configured implements Tool {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		try {
			int res = ToolRunner.run(conf, new PartitionerDriver(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public int run(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Partitioning File Based on Gender...........");
		if (args.length != 3) {
			System.err
					.println("Usage:File Partitioner <input> <output> <delimiter> ");
			System.exit(0);
		}
		Configuration conf = new Configuration();
		/*
		 * Arguments
		 */
		String source = args[0];
		String dest = args[1];
		String delimiter = args[2];
		
		//conf objects
		conf.set("delimiter", delimiter);
		
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Path in = new Path(source);
		Path out = new Path(dest);
		
		Job job0 = null;
		try {
			job0 = new Job(conf, "Partition Records");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		job0.setJarByClass(PartitionerDriver.class);
		job0.setMapperClass(PartitionMapper.class);
		job0.setReducerClass(PartitionReducer.class);
		job0.setMapOutputKeyClass(Text.class);
		job0.setMapOutputValueClass(Text.class);
		job0.setOutputKeyClass(Text.class);
		job0.setOutputValueClass(Text.class);
		try {
			TextInputFormat.addInputPath(job0, in);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		/*
		 * Delete output dir if exist
		 */
		try {
			if (fs.exists(out)) {
				fs.delete(out, true);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		TextOutputFormat.setOutputPath(job0, out);
		try {
			job0.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("Successfully partitioned Data based on Gender!");
		return 0;
	}
}
