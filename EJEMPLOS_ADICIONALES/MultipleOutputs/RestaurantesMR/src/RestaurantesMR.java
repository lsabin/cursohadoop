import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RestaurantesMR extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("USAGE: RestaurantesMR <input> <output>");
			return -1;
		}

		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(RestaurantesMR.class);
		job.setJobName("Restaurantes MR");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
		//job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setMapperClass(RestaurantesMapper.class);
		job.setReducerClass(RestaurantesPorValoracionReducer.class);

		job.setOutputKeyClass(MiShortWritable.class);
		job.setOutputValueClass(ParNombreDireccion.class);

		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new RestaurantesMR(), args);
		System.exit(exitCode);
	}

}
