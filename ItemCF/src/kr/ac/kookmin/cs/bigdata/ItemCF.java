package kr.ac.kookmin.cs.bigdata;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;


public class ItemCF extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        int res = ToolRunner.run(new Configuration(), new ItemCF(), args);
      
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));

        Job job = Job.getInstance(getConf());
        job.setJarByClass(ItemCF.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
      
        return 0;
    }
    
    
    public static class Map1 extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        
        @Override
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
            
        }
    }

    public static class Reduce1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
        throws IOException, InterruptedException {
        	
        }
    }
    
    public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
        
        @Override
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
            
        }
    }

    public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        	
        }
    }
    
    // Job3
    public static class Map3 extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
            
        }
    }

    public static class Reduce3 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
        throws IOException, InterruptedException {
        	
        }
    }
    
    // Job4
    public static class Map4 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        	
        }
    }

    public static class Reduce4 extends Reducer<Text, Text, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        	
        }
    }
    
    // Job5
    public static class Map5 extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        
        @Override
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        	
        }
    }

    public static class Reduce5 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
        throws IOException, InterruptedException {
        	
        }
    }
    
    // Job6
    public static class Map6 extends Mapper<LongWritable, Text, Text, Text> {
    	
        @Override
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        	
        }
    }

    public static class Reduce6 extends Reducer<Text, Text, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        	
        }
    }

   
    public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final static DoubleWritable ONE = new DoubleWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            	try{
            		JSONObject jsonObject = new JSONObject(value.toString());
            		word.set((String) jsonObject.get("asin"));
            		context.write(word, new DoubleWritable(jsonObject.getDouble("overall")));
            	}
              catch (JSONException j) {
              j.printStackTrace();
              }
            }
        }

    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
        		throws IOException, InterruptedException {
        	double sum = 0;
            int cnt = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                cnt ++;
            }
            
            context.write(key, new DoubleWritable(sum/cnt));
        
        }
    }
}
