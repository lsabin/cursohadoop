import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;


public class TestProcessLog {
	
	
	private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	
	
	@Before
	public void setup() {
		mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
		mapDriver.setMapper(new LogFileMapper());
		
		reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
		reduceDriver.setReducer(new LogFileReducer());
		
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>();
		mapReduceDriver.setMapper(new LogFileMapper());
		mapReduceDriver.setReducer(new LogFileReducer());
		
	}
	
	@Test
	public void testMap() {
		mapDriver.withInput(new LongWritable(0L), new Text("10.1.12.232 - aasda a  - asdasd  -  asdadad"));
		//mapDriver.withInput(new LongWritable(1L), new Text("10.1.12.233 - aasda a  - asdasd  -  asdadad"));
		
		//mapDriver.addOutput(new Text("10.1.12.233"), new IntWritable(1));
		mapDriver.addOutput(new Text("10.1.12.232"), new IntWritable(1));
		
		List<Pair<Text, IntWritable>> resultado = mapDriver.getExpectedOutputs();
		
		System.out.println("Resultado: " + resultado);
		
		
		mapDriver.runTest(false);
	}

	@Test
	public void testFailMap() throws Exception {
		mapDriver.withInput(new LongWritable(1), new Text("10.1.12.232 - aasda a  - asdasd  -  asdadad"))
			.withInput(new LongWritable(2), new Text("10.1 - aasda a  - asdasd  -  asdadad"));
		mapDriver.withOutput(new Text("10.1.12.232"), new IntWritable(1));
		
		List<Pair<Text, IntWritable>> resultado = mapDriver.getExpectedOutputs();
		
		System.out.println("Resultado: " + resultado);
		
		mapDriver.run();
	}
	
	
	@Test
	public void testReduce() {
		List<IntWritable> values1 = new ArrayList<IntWritable>();
		values1.add(new IntWritable(1));
		values1.add(new IntWritable(1));
		
		List<IntWritable> values2 = new ArrayList<IntWritable>();
		values2.add(new IntWritable(1));
		values2.add(new IntWritable(1));
		values2.add(new IntWritable(1));
		
		
		
		reduceDriver.withInput(new Text("10.1.12.232"), values1);
		reduceDriver.withInput(new Text("10.1.12.233"), values2);
		
		
		
		//reduceDriver.withOutput(new Text("10.1.12.233"), new IntWritable(3));
		//reduceDriver.withOutput(new Text("10.1.12.232"), new IntWritable(2));
		reduceDriver.addOutput(new Pair<Text,IntWritable>(new Text("10.1.12.233"), new IntWritable(3)));
		reduceDriver.addOutput(new Pair<Text,IntWritable>(new Text("10.1.12.232"), new IntWritable(2)));
		
		List<Pair<Text, IntWritable>> resultado = reduceDriver.getExpectedOutputs();
		
		System.out.println("Resultado reduce: " + resultado);
		
		
		reduceDriver.runTest();
		
	}
	
	
	@Test
	public void testMapReduce() {
		
		mapReduceDriver.withInput(new LongWritable(0L), new Text("10.1.12.232 - aasda a  - asdasd  -  asdadad"));
		mapReduceDriver.withInput(new LongWritable(1L), new Text("10.1.12.232 - aasda a  - asdasd  -  asdadad"));
		mapReduceDriver.withInput(new LongWritable(2L), new Text("10.1.12.233 - aasda a  - asdasd  -  asdadad"));
		
		mapReduceDriver.withOutput(new Text("10.1.12.232"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("10.1.12.233"), new IntWritable(1));
		
		List<Pair<Text, IntWritable>> resultado = mapReduceDriver.getExpectedOutputs();
		System.out.println("Resultado mapreduce: " + resultado);
		
		mapReduceDriver.runTest();
		
		String texto = "2012-01-02	12:12	Jersey City	Sporting Goods	465.04	MasterCard";
		String[] trozos = texto.split("\t");
		
		System.out.println("trozos: " + Arrays.toString(trozos));
		
		
	}
	

}
