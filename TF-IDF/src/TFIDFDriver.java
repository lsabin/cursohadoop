import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TFIDFDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf("Usage: TFIDFDriver <input dir> <output dir>\n");
			return -1;
		}	
		
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path ori = new Path(args[0]);
		conf.setInt("N", fs.listStatus(ori).length);
		//Conf 3 jobs
		Job jobTF = new Job(conf);
		jobTF.setJarByClass(TFIDFDriver.class);
		jobTF.setJobName("Computo de TF Shakespeare");
		
		Job job_n = new Job(conf);
		job_n.setJarByClass(TFIDFDriver.class);
		job_n.setJobName("Computo de n Shakespeare");
		
		Job jobTFIDF = new Job(conf);
		jobTFIDF.setJarByClass(TFIDFDriver.class);
		jobTFIDF.setJobName("Computo de TF-IDF Shakespeare");

		//Conf inputs outputs 3 jobs
		FileInputFormat.setInputPaths(jobTF, ori);
		FileOutputFormat.setOutputPath(jobTF, new Path("tf"));
		jobTF.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		FileInputFormat.setInputPaths(job_n, new Path("tf/part-r-00000"));
		FileOutputFormat.setOutputPath(job_n, new Path("computo_n"));
		job_n.setInputFormatClass(SequenceFileInputFormat.class);
		job_n.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		
		FileInputFormat.setInputPaths(jobTFIDF, new Path("computo_n/part-r-00000"));
		FileOutputFormat.setOutputPath(jobTFIDF, new Path(args[1]));
		jobTFIDF.setInputFormatClass(SequenceFileInputFormat.class);
		jobTFIDF.setOutputFormatClass(TextOutputFormat.class);
		
		//Conf mapper & reducers
		jobTF.setMapperClass(TFMapper.class);
		jobTF.setReducerClass(TFReducer.class);
			
		job_n.setMapperClass(ComputeNMapper.class);
		job_n.setReducerClass(ComputeNReducer.class);
		
		jobTFIDF.setMapperClass(ComputeTFIDFMapper.class);
		jobTFIDF.setReducerClass(Reducer.class);
		
		//Conf inputs outputs
		jobTF.setOutputKeyClass(ParTermDoc.class);
		jobTF.setOutputValueClass(IntWritable.class);

		job_n.setMapOutputKeyClass(Text.class);
		job_n.setMapOutputValueClass(DocTFCount.class);
		job_n.setOutputKeyClass(ParTermDoc.class);
		job_n.setOutputValueClass(TFN.class);
		
		jobTFIDF.setOutputKeyClass(ParTermDoc.class);
		jobTFIDF.setOutputValueClass(DoubleWritable.class);
		
		//Start all
		boolean success = jobTF.waitForCompletion(true);
		if (!success) return 1;
		success = job_n.waitForCompletion(true);
		if (!success) return 1;		
		success = jobTFIDF.waitForCompletion(true);
		if (!success) return 1;
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int r = ToolRunner.run(new TFIDFDriver(), args);
		System.exit(r);
	}
}
