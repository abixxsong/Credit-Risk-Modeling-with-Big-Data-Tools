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


public class avgincome {
	public static void main(String[] args) throws Exception { if (args.length != 2) {
		System.err.println("Usage: MaxTemperature <input path> <output path>");
		System.exit(-1);
	}
	Job job = new Job(); 
	job.setJarByClass(avgincome.class); 
	job.setJobName("Income avg");
	FileInputFormat.addInputPath(job, new Path(args[0])); FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setNumReduceTasks(1);
	job.setMapperClass(avgincomeMapper.class);
    job.setReducerClass(avgincomeReducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }

    static class avgincomeMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    	@Override
    	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    		String avg = value.toString();
            String[] words =avg.split(",");
            int length = 99;
    		if ((words.length ==length) & (avg.isEmpty() == false) & (!words[45].equals("NA")) & (!words[45].equals("income"))){
        
                double income = (Double.parseDouble(words[45])*1000)/12;
                context.write(new Text("Income avg"),new DoubleWritable(income));

            }
        }
    }

    static class avgincomeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
    	@Override
    	public void reduce(Text key, Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {
    		double avg = 0;
            double count = 0;

    		for(DoubleWritable value:values){
                avg += value.get();
                count += 1;
            }
            context.write(key, new DoubleWritable(avg/count));
        }
 }

}