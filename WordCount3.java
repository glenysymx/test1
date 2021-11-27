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

public class WordCount {

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
public static class WCPart extends Partitioner<Text, IntWritable>{
  @Override
  public int getPartition(Text key, IntWritable value, int numReduceTasks){
	  String myWord = key.toString();
	  if(myWord.startsWith("a") || myWord.startswith("b") || myWord.startsWith("c")){
		  return 0;
	  }
	  if(myWord.startsWith("d") || myWord.startswith("e") || myWord.startsWith("f")){
		  return 1;
	  }
	  if(myWord.startsWith("g") || myWord.startswith("h") || myWord.startsWith("i")){
		  return 2;
	  }
 	  if(myWord.startsWith("j") || myWord.startswith("k") || myWord.startsWith("l")){
		  return 3;
	  }
	  if(myWord.startsWith("m") || myWord.startswith("n") || myWord.startsWith("o")){
		  return 4;
	  }
	  if(myWord.startsWith("p") || myWord.startswith("q") || myWord.startsWith("r")){
		  return 5;
	  }
	  if(myWord.startsWith("s") || myWord.startswith("t") || myWord.startsWith("u")){
		  return 6;
	  }
	  if(myWord.startsWith("v") || myWord.startswith("w") || myWord.startsWith("x")){
		  return 7;
	  }
	  if(myWord.startsWith("y") || myWord.startswith("z")){
		  return 8;
	  } else {
		  return 9;
	  }
  }
}

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    
    job.setJarByClass(WordCount.class);
	  
    job.setMapperClass(WCMapper.class);
    job.setReducerClass(WCReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setNumReduceTasks(10);
	  
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
