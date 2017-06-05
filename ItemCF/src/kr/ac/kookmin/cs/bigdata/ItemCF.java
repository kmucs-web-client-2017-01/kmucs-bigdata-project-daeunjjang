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
        
        /* Get job-Item list to calculate each overall pair's InnerProduct */ 
        Job jobItemList = Job.getInstance(getConf());
        jobItemList.setJarByClass(ItemCF.class);
        jobItemList.setOutputKeyClass(Text.class);
        jobItemList.setOutputValueClass(Text.class);
        
        jobItemList.setMapperClass(MapItemList.class);
        jobItemList.setReducerClass(ReduceItemList.class);
        
        jobItemList.setInputFormatClass(TextInputFormat.class);
        jobItemList.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(jobItemList, new Path(inputPath));
        FileOutputFormat.setOutputPath(jobItemList, new Path(outputPath+"/itemList"));
        
        jobItemList.waitForCompletion(true);
        
        /* Get each overall pair's InnerProduct to calculate Similarity */
        Job jobInnerProduct = Job.getInstance(getConf());
        jobInnerProduct.setJarByClass(ItemCF.class);
        jobInnerProduct.setOutputKeyClass(Text.class);
        jobInnerProduct.setOutputValueClass(DoubleWritable.class);
        
        jobInnerProduct.setMapperClass(MapInnerProduct.class);
        jobInnerProduct.setReducerClass(ReduceInnerProduct.class);
        
        jobInnerProduct.setInputFormatClass(TextInputFormat.class);
        jobInnerProduct.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(jobInnerProduct, new Path(outputPath+"/itemList"));
        FileOutputFormat.setOutputPath(jobInnerProduct, new Path(outputPath+"/innerProduct"));
        
        jobInnerProduct.waitForCompletion(true);
        
        /* Get Similarity to get Item-CF rating */
        Job jobSimilarity = Job.getInstance(getConf());
        jobSimilarity.setJarByClass(ItemCF.class);
        MultipleInputs.addInputPath(jobSimilarity, new Path(outputPath+"/vectorSize"), TextInputFormat.class, MapSimilarityGetVectorSize.class);
        MultipleInputs.addInputPath(jobSimilarity, new Path(outputPath+"/innerProduct"), TextInputFormat.class, MapSmilarityGetInnerProduct.class);
        
        FileOutputFormat.setOutputPath(jobSimilarity, new Path(outputPath+"/similarity"));
        jobSimilarity.setReducerClass(ReduceSmilarity.class);
        jobSimilarity.setOutputKeyClass(Text.class);
        jobSimilarity.setOutputValueClass(Text.class);
        
        jobSimilarity.waitForCompletion(true);

        /* Get Overall Product Similarity to get Item-CF rating */
        Job jobOverallProductSimilarity = Job.getInstance(getConf());
        jobOverallProductSimilarity.setJarByClass(ItemCF.class);
        MultipleInputs.addInputPath(jobOverallProductSimilarity, new Path(inputPath), TextInputFormat.class, MapOverallProductSimilarityGetOverall.class);
        MultipleInputs.addInputPath(jobOverallProductSimilarity, new Path(outputPath+"/similarity"), TextInputFormat.class, MapOverallProductSimilarityGetSimilarity.class);
        
        FileOutputFormat.setOutputPath(jobOverallProductSimilarity, new Path(outputPath+"/overallProductSimilarity"));
        jobOverallProductSimilarity.setReducerClass(ReduceOverallProductSimilarity.class);
        jobOverallProductSimilarity.setOutputKeyClass(Text.class);
        jobOverallProductSimilarity.setOutputValueClass(DoubleWritable.class);
        
        jobOverallProductSimilarity.waitForCompletion(true);

        /* Exclude empty review case from Item-Item CF calculate */
        Job jobBlankFiltering = Job.getInstance(getConf());
        jobBlankFiltering.setJarByClass(ItemCF.class);
        MultipleInputs.addInputPath(jobBlankFiltering, new Path(outputPath+"/overallProductSimilarity"), TextInputFormat.class, MapBlankFilteringGetOverallProductSimilarity.class);
        MultipleInputs.addInputPath(jobBlankFiltering, new Path(outputPath+"/similarity"), TextInputFormat.class, MapBlankFilteringGetSimilarity.class);
        
        FileOutputFormat.setOutputPath(jobBlankFiltering, new Path(outputPath+"/blankFiltering"));
        jobBlankFiltering.setReducerClass(ReduceBlankFiltering.class);
        jobBlankFiltering.setOutputKeyClass(Text.class);
        jobBlankFiltering.setOutputValueClass(Text.class);
        
        jobBlankFiltering.waitForCompletion(true);
        
        /* Calculate how this customer rate the item by Item-Item CF rating. */
        Job jobRating = Job.getInstance(getConf());
        jobRating.setJarByClass(ItemCF.class);
        jobRating.setOutputKeyClass(Text.class);
        jobRating.setOutputValueClass(Text.class);
        
        jobRating.setMapperClass(MapRating.class);
        jobRating.setReducerClass(ReduceRaiting.class);
        
        jobRating.setInputFormatClass(TextInputFormat.class);
        jobRating.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(jobRating, new Path(outputPath+"/blankFiltering"));
        FileOutputFormat.setOutputPath(jobRating, new Path(outputPath+"/result"));
        
        jobRating.waitForCompletion(true);
        
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
        	String asin;
        	String overall;
        	String reviewerid;
        	
            for (String token: value.toString().split("\n")) {
                try {
                    JSONObject jsonObject = new JSONObject(token);
                    
                    reviewerid = jsonObject.getString("reviewerID");
                    asin = jsonObject.getString("asin").toString();
                    overall = jsonObject.get("overall").toString();
                    		
                    reviewerID.set(reviewerid);
                    asinScore.set(asin + "," + overall);
                    
                    context.write(reviewerID, asinScore);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static class ReduceItemList extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        	String asinScores = "";
        	
            for (Text val : values)
            	asinScores += val.toString() + " ";
            
            context.write(key, new Text(asinScores));
        }
    }
    
    // jobInnerProduct
    public static class MapInnerProduct extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private String asin;
        
        @Override
        protected void setup(Mapper.Context context)
        throws IOException, InterruptedException {
            
            // get the searchingWord from configuration
            asin = context.getConfiguration().get("asin");
        }
        
        @Override
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
            String[] userAsinScores;
            String asinScores;
            
            userAsinScores = value.toString().split("\t");
            asinScores = userAsinScores[1];
            
            String[] asinScore = asinScores.split(" ");
            
            double overall1 = 0;
            double overall2 = 0;
            
            for(int i=0; i<asinScore.length; i++){
                String[] tmp1 = asinScore[i].split(",");
                for(int j=i+1; j<asinScore.length; j++){
                    String[] tmp2 = asinScore[j].split(",");
                    
                    if(tmp2[0].equals(asin))
                        tmp2[0] = tmp1[0];
                    else if(!tmp1[0].equals(asin))
                        continue;
                    
                    overall1 = Double.parseDouble(tmp1[1]);
                    overall2 = Double.parseDouble(tmp2[1]);
                    
                    context.write(new Text(tmp2[0]), new DoubleWritable(overall1*overall2));
                }
            }
        }
    }
    
    public static class ReduceInnerProduct extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
        throws IOException, InterruptedException {
            double overallSum = 0.0F;
            
            for (DoubleWritable val : values)
                overallSum += val.get();
            
            context.write(key, new DoubleWritable(overallSum));
        }
    }
    
    // jobSimilarity
    public static class MapSimilarityGetVectorSize extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
            String[] asinScore = value.toString().split("\t");
            
            context.write(new Text(asinScore[0]), new Text("VectorSize,"+asinScore[1]));
        }
    }
    
    public static class MapSmilarityGetInnerProduct extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
            String[] asinScore = value.toString().split("\t");
            
            context.write(new Text(asinScore[0]), new Text("InnerProduct,"+asinScore[1]));
        }
    }
    
    public static class ReduceSmilarity extends Reducer<Text, Text, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
            double similarity = 0;
            double vectorSize = 0;
            double innerProduct = 0;
            
            for (Text val : values){
                String[] mapScore = val.toString().split(",");
                
                if(mapScore[0].equals("VectorSize"))
                    vectorSize = Double.parseDouble(mapScore[1]);
                else if(mapScore[0].equals("InnerProduct"))
                    innerProduct += Double.parseDouble(mapScore[1]);
            }
            
            if (innerProduct == 0)
            	return ;
            
            similarity = innerProduct / vectorSize;
            
            context.write(key, new DoubleWritable(similarity));
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
            String[] userRating = value.toString().split("\t");
        	
        	context.write(new Text(userRating[0]), new Text("OverallProductSimilarity," + userRating[1]));
        }
    }
    
    public static class MapBlankFilteringGetSimilarity extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        	String[] asinScore = value.toString().split("\t");
        	
        	context.write(new Text(asinScore[0]), new Text("Similarity," + asinScore[1]));
        }
    }

    public static class ReduceBlankFiltering extends Reducer<Text, Text, Text, Text> {
        private final int EMPTY_REVIEW = -1;
        
        public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        	double overallProductSimilarity = 0;
        	double similarity = 0;
        	
            for (Text val : values) {
            	String[] mapScore = val.toString().split(",");
            	
            	if(mapScore[0].equals("OverallProductSimilarity"))
            		overallProductSimilarity = Double.parseDouble(mapScore[1]);
            	else if(mapScore[0].equals("Similarity"))
            		similarity = Double.parseDouble(mapScore[1]);
            	
            	if(overallProductSimilarity == EMPTY_REVIEW)
            		return ;
            }
            
            Configuration conf = context.getConfiguration();
            
            context.write(new Text(conf.get("user")), new Text(overallProductSimilarity+","+similarity));
        }
    }
    
    // jobRating
    public static class MapRating extends Mapper<LongWritable, Text, Text, Text> {
            @Override
            public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
                String[] userScore = value.toString().split("\t");
            	String user = userScore[0];
            	String score = userScore[1];
            	
            	context.write(new Text(user), new Text(score));
            }
        }

    public static class ReduceRaiting extends Reducer<Text, Text, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        	double overallProductSimilaritySum = 0;
        	double similaritySum = 0;
        	
            for (Text val : values) {
            	String[] numerDenno = val.toString().split(",");
            	
            	overallProductSimilaritySum += Double.parseDouble(numerDenno[0]);
            	similaritySum += Double.parseDouble(numerDenno[1]);
            }
            
            double rating = overallProductSimilaritySum / similaritySum;
            
            Configuration conf = context.getConfiguration();
            
            context.write(new Text("(" + key + "," + conf.get("asin") + ")"), new DoubleWritable(rating));
        }
    }
}
