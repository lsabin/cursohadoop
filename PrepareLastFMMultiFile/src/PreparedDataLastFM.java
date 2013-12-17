import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class PreparedDataLastFM extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf("Usage: PreparedDataLastFM <input dir> <output dir>  \n ");
			return -1;
		}
		Job job = new Job();
		job.setJarByClass(PreparedDataLastFM.class);
		job.setJobName("LastFm data prepared");
		MyCombineInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setInputFormatClass(MyCombineInputFormat.class);
		job.setMapperClass(CleanerMapper.class);
		job.setReducerClass(IdentityReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		
		int exitCode = ToolRunner.run(new PreparedDataLastFM(), args);
		System.exit(exitCode);
	}
}
