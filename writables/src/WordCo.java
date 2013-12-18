import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCo extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.printf("Usage: WordCo <input dir> <output dir>\n");
      return -1;
    }

    Job job = new Job(getConf());
    job.setJarByClass(WordCo.class);
    job.setJobName("Custom Writable Comparable");
    
    
    Job jobSort = new Job(getConf());
    jobSort.setJarByClass(WordCo.class);
    jobSort.setJobName("Custom Writable Comparable Sort");
    
    
    
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    

	FileInputFormat.setInputPaths(jobSort, new Path(args[1] + "/part-r-00000"));
	FileOutputFormat.setOutputPath(jobSort, new Path(args[1] + "/sort"));    
    

    /*
     * Specify the mapper and reducer classes.
     */
    job.setMapperClass(WordCoMapper.class);
    job.setReducerClass(SumReducer.class);
    
    job.setMapOutputKeyClass(StringPairWritable.class);
    job.setMapOutputValueClass(LongWritable.class);
    
    job.setOutputKeyClass(StringPairWritable.class);
    job.setOutputValueClass(LongWritable.class);
    
    
    jobSort.setMapperClass(SortMapper.class);
    jobSort.setNumReduceTasks(0);
    
    jobSort.setOutputKeyClass(LongWritable.class);
    jobSort.setOutputValueClass(StringPairWritable.class);
    
    

    
	boolean success = job.waitForCompletion(true);
	if (!success) return 1;
	
	success = jobSort.waitForCompletion(true);
	if (!success) return 1;		
	
	return 0;

  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new WordCo(), args);
    System.exit(exitCode);
  }
}
