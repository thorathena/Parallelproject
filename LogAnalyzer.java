
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.regex.*;

public class LogAnalyzer {

	public static class LogEntryMapper extends Mapper<Object, Text, Text, IntWritable> {
		
		private final static IntWritable one = new IntWritable(1);
		private Text url = new Text();
		
		private Pattern p = Pattern.compile("(?:GET|POST)\\s([^\\s]+)");
		
		public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
			String[] entries = value.toString().split("\r?\n"); 
			for (int i=0, len=entries.length; i<len; i+=1) {
				Matcher matcher = p.matcher(entries[i]);
				if (matcher.find()) {
					url.set(matcher.group(1));
					context.write(url, one);
				}
			}
		}
	}
	
	public static class LogEntryReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		private IntWritable total = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
			int sum = 0;
		    for (IntWritable value : values) {
		    	sum += value.get();
		    }
		    total.set(sum);
		    context.write(key, total);
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		if (args.length != 2) {
			System.err.println("Usage: loganalyzer <in> <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "analyze log");
		job.setJarByClass(LogAnalyzer.class);
		job.setMapperClass(LogEntryMapper.class);
		job.setReducerClass(LogEntryReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
