import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
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


public class minincome {
	public static void main(String[] args) throws Exception { if (args.length != 2) {
		System.err.println("Usage: minTemperature <input path> <output path>");
		System.exit(-1);
	}
	Job job = new Job(); 
	job.setJarByClass(minincome.class); 
	job.setJobName("Income min");
	FileInputFormat.addInputPath(job, new Path(args[0])); FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setNumReduceTasks(1);
	job.setMapperClass(minincomeMapper.class);
    job.setReducerClass(minincomeReducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }

    static class minincomeMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    	@Override
    	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    		String min = value.toString();
            String[] words =min.split(",");
            int length = 99;
    		if ((words.length ==length) & (min.isEmpty() == false) & (!words[45].equals("NA")) & (!words[45].equals("income"))){
        
                double income = (Double.parseDouble(words[45])*1000)/12;
                context.write(new Text("min Income"),new DoubleWritable(income));

            }
        }
    }

    static class minincomeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
    	@Override
    	public void reduce(Text key, Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {
    		double minincome = Double.MAX_VALUE;

    		for(DoubleWritable value:values){
                minincome = Math.min(minincome, value.get());
            }

            context.write(key, new DoubleWritable(minincome));
        }
 }

}