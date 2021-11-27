import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class WordCount3 extends Configured implements Tool{

//mapper class
  public static class WCMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
	  
    static int cnt = 0;
    List ls = new ArrayList();
	  
    @SuppressWarnings("unchecked")
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), " ");
	    

      //Tokenised strings contain punctuations, e.g. comma, and fullstop
      //Your task is to ensure that only words themselves are used as keys

      while (itr.hasMoreTokens()) {
	ls.add(itr.nextToken());
      }
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	StringBuffer str = new StringBuffer("");
	for (int i = 0; i < ls.size() - 1; i++) {
		int k=i;
		for(int j=0;j<2;j++) {
			if(j==0) {
				str = str.append(ls.get(k));
				str = str.append(" ");
			} else 
			{
				str = str.append(ls.get(k));
			}
			k++;
		}
		word.set(str.toString());
		str=new StringBuffer("");
		context.write(word,one);
	}
    }
  }

	//reducer class
  public static class WCReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
public static class WCPart extends Partitioner<Text,IntWritable>{
	
  @Override
  public int getPartition(Text key, IntWritable value, int numReduceTasks){
	  String myWord = key.toString();
	  if(myWord.startsWith("a") || myWord.startsWith("b") || myWord.startsWith("c")){
		  return 0;
	  }
	  if(myWord.startsWith("d") || myWord.startsWith("e") || myWord.startsWith("f")){
		  return 1;
	  }
	  if(myWord.startsWith("g") || myWord.startsWith("h") || myWord.startsWith("i")){
		  return 2;
	  }
 	  if(myWord.startsWith("j") || myWord.startsWith("k") || myWord.startsWith("l")){
		  return 3;
	  }
	  if(myWord.startsWith("m") || myWord.startsWith("n") || myWord.startsWith("o")){
		  return 4;
	  }
	  if(myWord.startsWith("p") || myWord.startsWith("q") || myWord.startsWith("r")){
		  return 5;
	  }
	  if(myWord.startsWith("s") || myWord.startsWith("t") || myWord.startsWith("u")){
		  return 6;
	  }
	  if(myWord.startsWith("v") || myWord.startsWith("w") || myWord.startsWith("x")){
		  return 7;
	  }
	  if(myWord.startsWith("y") || myWord.startsWith("z")){
		  return 8;
	  } else {
		  return 9;
	  }
  }
}

  @Override

  public int run(String[] arg) throws Exception{
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
	
    FileInputFormat.addInputPath(job, new Path(arg[0]));
    FileOutputFormat.setOutputPath(job, new Path(arg[1]));
	  
    job.setMapperClass(WCMapper.class);
	  
    job.setPartitionerClass(WCMap.class);
    
    job.setReducerClass(WCReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setNumReduceTasks(10);
	  

    job.setOutputValueClass(IntWritable.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
    return 0;
	  
  }
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new WordCount3(),ar);
    System.exit(0);
	  


  }
}

