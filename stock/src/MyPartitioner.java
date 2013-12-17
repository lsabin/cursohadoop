import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner<K2, V2> extends Partitioner<IdProductoDiaWritable, DiaStockWritable> implements
    Configurable {

  private Configuration configuration;


  @Override
  public void setConf(Configuration configuration) {
  }


  @Override
  public Configuration getConf() {
    return configuration;
  }


  public int getPartition(IdProductoDiaWritable key, DiaStockWritable value, int numReduceTasks) {
	  
	  return key.getId().hashCode() % numReduceTasks;
	  
  }
}
