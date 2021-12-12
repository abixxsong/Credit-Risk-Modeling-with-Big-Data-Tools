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


public class NARecords {
	public static void main(String[] args) throws Exception { if (args.length != 2) {
		System.err.println("Usage: MaxTemperature <input path> <output path>");
		System.exit(-1);
	}
	Job job = new Job(); 
	job.setJarByClass(NARecords.class); 
	job.setJobName("Reocrds NA");
	FileInputFormat.addInputPath(job, new Path(args[0])); FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setNumReduceTasks(1);
	job.setMapperClass(NARecsMapper.class);
    job.setReducerClass(NARecsReducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }

    static class NARecsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    	@Override
    	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    		String line = value.toString();
            String[] words = line.split(",");
            int length = 99;
            int countNA = 0;
    		for (int i = 0; i < length; i++){
                if (words[i].equals("NA")){
                    countNA += 1;
                }

            context.write(new Text("NA Records"),new IntWritable(countNA));


            }
        }
    }

    static class NARecsReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
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