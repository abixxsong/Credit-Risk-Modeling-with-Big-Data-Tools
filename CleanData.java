import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CleanData {
	public static void main(String[] args) throws Exception { if (args.length != 2) {
		System.err.println("Usage: MaxTemperature <input path> <output path>");
		System.exit(-1);
	}
	Job job = new Job(); 
	job.setJarByClass(Clean.class); 
	job.setJobName("Clean Data");
	FileInputFormat.addInputPath(job, new Path(args[0])); FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setNumReduceTasks(1);
	job.setMapperClass(CleanDataMapper.class);
    job.setReducerClass(CleanDataReducer.class);
    
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }

    static class CleanDataMapper extends Mapper<LongWritable, Text, NullWritable,Text> {
    	@Override
    	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    		String line = value.toString();
    		String[] words = line.split(",");
    		int length = 99;
    		if ((words.length == length) & (!words[77].equals("8888")) & (!words[73].equals("3")) & (!words[73].equals("4")) & (!words[73].equals("6")) & (!words[88].equals("1111")) & (!words[45].equals("NA")) & (!words[45].equals("income"))){
    			//range of <25,>74,25-34,35-44,45-54,55-64,65-74
    			String age;
                if(words[77].equals("<25")){
                    age = "1";
                }
                else if(words[77].equals("25-34")){
                    age = "2";
                }
                else if(words[77].equals("35-44")){
                    age = "3";
                }
                else if(words[77].equals("45-54")){
                    age = "4";
                }
                else if(words[77].equals("55-64")){
                    age = "5";
                }
                else if (words[77].equals("65-74")){
                    age = "6";
                }
                else {
                    age = "7";
                }

    			//1 for male and 0 for female
    			String sex;
                if (words[73].equals("1")){
                    sex = "1";
                }
                else{
                    sex = "0";
                }
            
    			
    			//Units >0 consider as having own housing property
                String housing;
                if (!words[96].equals("0")){
    				housing = "1";
    			}
    			else{
    				housing = "0";
    			}
            

                //has income > 0, 1 for has job, 0 for has no job
                String employment;
                if (Integer.parseInt(words[45]) > 0){
    				employment = "1";
    			}
    			else{    
    				employment = "0";
    			}

    			//income ratio
                String income;
                double avg = 13612.32641493288;
                double inco = (Double.parseDouble(words[45]))/avg;
                income = Double.toString(inco);
                
    			//denial reason 10 as good credit socre (1), 2 for bad credit score
                String credit; 
                if (words[88].equals("10")){
    				credit = "1";
    			}
    			else{
    				credit = "0";
    			}

    			Text result = new Text(housing+',' + age + ',' + sex + ',' +employment+ ',' + income + ',' + credit);
                context.write(NullWritable.get(),result);
            }     
        }
    }

    static class CleanDataReducer extends Reducer<NullWritable, Text, NullWritable,Text>{
    	public void reduce(NullWritable key, Text values,Context context) throws IOException, InterruptedException {
    		context.write(NullWritable.get(),values);
    	}
    }
}