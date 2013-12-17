import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;


public class StockCount {

  public static void main(String[] args) throws Exception {


    if (args.length != 2) {
      System.out.printf(
          "Usage: StockCount <input dir> <output dir>\n");
      System.exit(-1);
    }

    Job job = new Job();
    job.setJarByClass(StockCount.class);
    job.setJobName("StockCount");

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    /*
     * Specify the mapper and reducer classes.
     */
    job.setMapperClass(StockMapper.class);
    job.setReducerClass(StockReducer.class);
    job.setPartitionerClass(MyPartitioner.class);
    
    
    job.setSortComparatorClass(PrimerComparator.class);
    job.setGroupingComparatorClass(SegundoComparator.class);
    
    job.setMapOutputKeyClass(IdProductoDiaWritable.class);
    job.setMapOutputValueClass(DiaStockWritable.class);
    
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    
//    job.setOutputKeyClass(IdProductoDiaWritable.class);
//    job.setOutputValueClass(DiaStockWritable.class);
    
    
    job.setNumReduceTasks(3);


    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0 : 1);
  }
}
