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
	for (int i = 0; i < ls.size() - 3; i++) {
		int k=i;
		for(int j=0;j<3;j++) {
			if(j>0) {
				str = str.append(" ");
				str = str.append(ls.get(k));
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

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(WCMapper.class);
    job.setReducerClass(WCReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

