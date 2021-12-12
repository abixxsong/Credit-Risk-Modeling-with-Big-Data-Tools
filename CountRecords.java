import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.Iterator;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;


public class CountRecords {
	public static void main(String[] args) throws Exception { if (args.length != 2) {
		System.err.println("Usage: MaxTemperature <input path> <output path>");
		System.exit(-1);
	}
	Job job = new Job(); 
	job.setJarByClass(CountRecords.class); 
	job.setJobName("Reocrds Count");
	FileInputFormat.addInputPath(job, new Path(args[0])); FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setNumReduceTasks(1);
	job.setMapperClass(CountRecsMapper.class);
    job.setReducerClass(CountRecsReducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }

    static class CountRecsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    	@Override
    	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    		String line = value.toString();
    		if (line.isEmpty() == false){
    			context.write(new Text("Record Lines"),new IntWritable(1));
            }
        }
    }

    static class CountRecsReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    	@Override
    	public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
    		int count = 0;
    		for(IntWritable value:values){
			count+=value.get();
		}
		context.write(key, new IntWritable(count));
	}
}




}