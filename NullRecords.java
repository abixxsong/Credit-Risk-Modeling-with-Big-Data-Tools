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


public class NullRecords {
	public static void main(String[] args) throws Exception { if (args.length != 2) {
		System.err.println("Usage: MaxTemperature <input path> <output path>");
		System.exit(-1);
	}
	Job job = new Job(); 
	job.setJarByClass(NullRecords.class); 
	job.setJobName("Reocrds Null");
	FileInputFormat.addInputPath(job, new Path(args[0])); FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setNumReduceTasks(1);
	job.setMapperClass(NullRecsMapper.class);
    job.setReducerClass(NullRecsReducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }

    static class NullRecsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    	@Override
    	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    		String line = value.toString();
            String[] words = line.split(",");
            int length = 99;
            int null_ = 0;
            String number_null;
    		for (int i = 0; i < length; i++){
                if (words[i].equals("Null")){
                    null_ += 1;
                }

            context.write(new Text("Null Records"),new IntWritable(null_));


            }
        }
    }

    static class NullRecsReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
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