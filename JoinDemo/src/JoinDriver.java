import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


 
/**
 * Usage: JoinDriver joinType(map or reduce) set1 set2 output. * 
 */

public class JoinDriver extends Configured implements Tool {
    

    public void reduceJoin(String set1, String set2, String output) throws Exception {
        Job job = new Job(getConf());
        job.setJarByClass(JoinDriver.class);
        MultipleInputs.addInputPath(job, new Path(set1), TextInputFormat.class, ReduceJoinMapperGlossary.class);
        MultipleInputs.addInputPath(job, new Path(set2), TextInputFormat.class, ReduceJoinMapperDocs.class);
        FileOutputFormat.setOutputPath(job, new Path(output));
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);
        
    }
   
    
    public void mapJoin(String set1, String set2, String output) throws Exception {
        Configuration myConf = getConf();
        myConf.set("set1", set1);
        Job job = new Job(myConf);
        job.setJobName("Map Side Join Demo");
        FileInputFormat.setInputPaths(job, new Path(set2));
        FileOutputFormat.setOutputPath(job, new Path(output));
        DistributedCache.addCacheFile(new URI(set1), job.getConfiguration());
        job.setJarByClass(JoinDriver.class);
        job.setMapperClass(MapJoinMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);
    }
    
    public int run(String[] args) throws Exception {
        String joinType;
        if (args.length != 4) {
            System.out.println(
                    "Usage: hadoop jar JoinDriver joinType(map or reduce) set1 set2 output.\n");
            return -1;
        }
        joinType = args[0];
        if (joinType.equals("map")) {
            mapJoin(args[1], args[2], args[3]);
        }
        if (joinType.equals("reduce")) {
            reduceJoin(args[1], args[2], args[3]);
        }
        return 1;
    }
    
    public static void main (String[] args) throws Exception {
        System.exit(ToolRunner.run(new JoinDriver(), args));
    }
}

