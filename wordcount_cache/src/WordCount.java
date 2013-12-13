
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.RegexMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {
	
	

  @Override
	public int run(String[] args) throws Exception {
	  

	    /*
	     * The expected command-line arguments are the paths containing
	     * input and output data. Terminate the job if the number of
	     * command-line arguments is not exactly 2.
	     */
	    if (args.length != 2) {
	      System.out.printf(
	          "Usage: WordCount <input dir> <output dir>\n");
	      return -1;
	    }
	    
	    String valorParam = getConf().get("parametro");
	    System.out.println("valor parametro: " + valorParam);
	    
   	    

	    Job job = new Job(getConf());
	    job.setJarByClass(WordCount.class);
	    job.setJobName("Word Count con Cache");
	    
	    
	    //Guarda el fichero en la cache distribuida
	    DistributedCache.addCacheFile(
	    		new URI("file:///home/training/training_materials/developer/data/shakespeare/glossary"), 
	    		job.getConfiguration());

	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    /*
	     * Specify the mapper and reducer classes.
	     */
	    job.setMapperClass(WordMapper.class);
	    job.setReducerClass(SumReducer.class);
	    //job.setPartitionerClass(MyPartitioner.class);
	    //job.setNumReduceTasks(4);

	    /*
	     * Specify the job's output key and value classes.
	     */
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);

	    /*
	     * Start the MapReduce job and wait for it to finish.
	     * If it finishes successfully, return 0. If not, return 1.
	     */
	    boolean success = job.waitForCompletion(true);
	    return (success ? 0 : 1);	  
	  

	}

public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
  	int exitCode = ToolRunner.run(conf, new WordCount(), args);
  	
  	System.exit(exitCode);

  }
}
