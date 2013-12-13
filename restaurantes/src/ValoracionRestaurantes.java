
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ValoracionRestaurantes extends Configured implements Tool {
	
	

  @Override
	public int run(String[] args) throws Exception {
	    if (args.length != 2) {
	      System.out.printf(
	          "Usage: ValoracionRestaurantes <input dir> <output dir>\n");
	      return -1;
	    }
	    

	    Job job = new Job(getConf());
	    job.setJarByClass(ValoracionRestaurantes.class);
	    job.setJobName("ValoracionRestaurantes");
	    
	    
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));

	    job.setMapperClass(RestaurantesMapper.class);
	    //job.setReducerClass(RestaurantesReducer.class);
	    //job.setNumReduceTasks(4);
	    
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);

	    job.setOutputKeyClass(InversoShortWritable.class);
	    job.setOutputValueClass(RestauranteWritable.class);

	    /*
	     * Start the MapReduce job and wait for it to finish.
	     * If it finishes successfully, return 0. If not, return 1.
	     */
	    boolean success = job.waitForCompletion(true);
	    return (success ? 0 : 1);	  
	  

	}

public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
  	int exitCode = ToolRunner.run(conf, new ValoracionRestaurantes(), args);
  	
  	System.exit(exitCode);

  }
}
