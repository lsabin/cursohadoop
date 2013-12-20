import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Map<String, String> smallSet = new HashMap<String, String>();

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		Path[] paths = DistributedCache.getLocalCacheFiles(context
				.getConfiguration());
		BufferedReader reader = new BufferedReader(new FileReader(
				paths[0].toString()));
		String line = null;
		try {
			while ((line = reader.readLine()) != null) {
				String[] fields = line.split("\t");
				if (fields.length >= 2) {
					if (fields[0].length() > 1) {
						smallSet.put(fields[0].toUpperCase(), fields[1]);
					}
				}
			}
		} finally {
			if (reader != null)
				reader.close();
		}
	}

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String valString = value.toString();
		String[] words = valString.split("\\W+");

		for (String word : words) {
			String def = smallSet.get(word.toUpperCase());
			if ((def != null) && (def.length() > 1)) {
				context.write(new Text(word), new Text(def));
			}
		}

	}
}
