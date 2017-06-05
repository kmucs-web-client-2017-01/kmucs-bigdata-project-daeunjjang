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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONObject;

public class ItemCF extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        
        Configuration conf = new Configuration();
        conf.set("user", args[2]);
        conf.set("asin", args[3]);
        
        int res = ToolRunner.run(conf, new ItemCF(), args);
        
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        
        String inputPath  = args[0];
        String outputPath = args[1];
        
        /* Get each item VectorSize to calculate Similarity */
        Job jobVectorSize = Job.getInstance(getConf());
        jobVectorSize.setJarByClass(ItemCF.class);
        jobVectorSize.setOutputKeyClass(Text.class);
        jobVectorSize.setOutputValueClass(DoubleWritable.class);
        
        jobVectorSize.setMapperClass(MapVectorSize.class);
        jobVectorSize.setReducerClass(ReduceVectorSize.class);
        
        jobVectorSize.setInputFormatClass(TextInputFormat.class);
        jobVectorSize.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(jobVectorSize, new Path(inputPath));
        FileOutputFormat.setOutputPath(jobVectorSize, new Path(outputPath+"/vectorSize"));
        
        jobVectorSize.waitForCompletion(true);
        
        return 0;
    }
    
    // jobVectorSize
    public static class MapVectorSize extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    	private Text asin = new Text();
    	private DoubleWritable overall = new DoubleWritable();
        
        @Override
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
            try {
                JSONObject jsonObject = new JSONObject(value.toString());
                
                asin.set(jsonObject.getString("asin"));
                overall.set(jsonObject.getDouble("overall"));
                
                context.write(asin, overall);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class ReduceVectorSize extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
        throws IOException, InterruptedException {
        	double squareSum = 0.0F;
            
            for (DoubleWritable val : values){
                double tmp = val.get();
                squareSum += tmp*tmp;
            }
            
            double sqrtSquareSum = Math.sqrt(squareSum);
            context.write(key, new DoubleWritable(sqrtSquareSum));
        }
    }
    
    // jobItemList
    public static class MapItemList extends Mapper<LongWritable, Text, Text, Text> {
    	private Text reviewerID = new Text();
    	private Text asinScore = new Text();
    	
        
        @Override
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        	
        }
    }

    public static class ReduceItemList extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        	
        }
    }
    
    // jobInnerProduct
    public static class MapInnerProduct extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    	private String asin;
    	
    	@Override
        protected void setup(Mapper.Context context)
            throws IOException, InterruptedException {
                
        }
    	
        @Override
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        	
        }
    }

    public static class ReduceInnerProduct extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
        throws IOException, InterruptedException {
        	
        }
    }
    
    // jobSimilarity
    public static class MapSimilarityGetVectorSize extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        	
        }
    }
    
    public static class MapSmilarityGetInnerProduct extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        	
        }
    }

    public static class ReduceSmilarity extends Reducer<Text, Text, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        	
        }
    }
    
    // jobOverallProductSimilarity
    public static class MapOverallProductSimilarityGetOverall extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text asin = new Text();
        private DoubleWritable overall = new DoubleWritable();
        
        private String user;
        
        @Override
        protected void setup(Mapper.Context context)
        throws IOException, InterruptedException {
            
            // get the searchingWord from configuration
            user = context.getConfiguration().get("user");
        }
        
        @Override
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
            try {
                JSONObject jsonObject = new JSONObject(value.toString());
                
                // ignore it if it's not my review
                if(!jsonObject.getString("reviewerID").equals(user))
                    return ;
                
                asin.set(jsonObject.getString("asin"));
                overall.set(jsonObject.getDouble("overall"));
                
                context.write(asin, overall);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    public static class MapOverallProductSimilarityGetSimilarity extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
            String[] asinSimilarity = value.toString().split("\t");
            String asin = asinSimilarity[0];
            String similarity = asinSimilarity[1];
            
            context.write(new Text(asin), new DoubleWritable(Double.parseDouble(similarity)));
        }
    }
    
    public static class ReduceOverallProductSimilarity extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private final int EMPTY_REVIEW = -1;
        
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
        throws IOException, InterruptedException {
            double rating_numer = 1;
            
            int count = 0;
            for (DoubleWritable val : values){
                rating_numer *= val.get();
                count++;
            }
            
            if(count == 1)
                rating_numer = EMPTY_REVIEW;
            
            context.write(new Text(key), new DoubleWritable(rating_numer));
        }
    }
    
    // jobBlankFiltering
    public static class MapBlankFilteringGetOverallProductSimilarity extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        	
        }
    }
    
    public static class MapBlankFilteringGetSimilarity extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        	
        }
    }

    public static class ReduceBlankFiltering extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        	
        }
    }
    
    // jobRating
    public static class MapRating extends Mapper<LongWritable, Text, Text, Text> {
            @Override
            public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
                
            }
        }

    public static class ReduceRaiting extends Reducer<Text, Text, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        	
        }
    }
}
