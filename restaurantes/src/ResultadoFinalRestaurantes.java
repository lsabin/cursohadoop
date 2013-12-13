
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ResultadoFinalRestaurantes extends Configured implements Tool {
	
	

  @Override
	public int run(String[] args) throws Exception {
	    if (args.length != 2) {
	      System.out.printf(
	          "Usage: ResultadoFinalRestaurantes <input dir> <output dir>\n");
	      return -1;
	    }
	    

	    Job job = new Job(getConf());
	    job.setJarByClass(ResultadoFinalRestaurantes.class);
	    job.setJobName("ResultadoFinalRestaurantes");
	    
	    
	    SequenceFileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    job.setInputFormatClass(SequenceFileInputFormat.class);


	    job.setMapperClass(RestaurantesProvinciaMapper.class);
    

	    job.setOutputKeyClass(ProvinciaDireccionWritable.class);
	    job.setOutputValueClass(Text.class);

	    /*
	     * Start the MapReduce job and wait for it to finish.
	     * If it finishes successfully, return 0. If not, return 1.
	     */
	    boolean success = job.waitForCompletion(true);
	    return (success ? 0 : 1);	  
	  

	}

public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
  	int exitCode = ToolRunner.run(conf, new ResultadoFinalRestaurantes(), args);
  	
  	System.exit(exitCode);

  }
}
