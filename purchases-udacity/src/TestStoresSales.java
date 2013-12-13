import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;


public class TestStoresSales {
	
	
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
	
	
	@Before
	public void setup() {
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper(new StoreMapper());
		
		
		reduceDriver = new ReduceDriver<Text, DoubleWritable, Text, DoubleWritable>();
		reduceDriver.setReducer(new StoreReducer());
		
		
		
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable>();
		mapReduceDriver.setMapper(new StoreMapper());
		mapReduceDriver.setReducer(new StoreReducer());
		
	}
	
	@Test
	public void testMap() {
		String texto = "2012-01-02	12:12	Jersey City	Sporting Goods	465.04	MasterCard";
		mapDriver.withInput(new LongWritable(1), new Text(texto));
		mapDriver.addOutput(new Pair<Text,DoubleWritable>(new Text("Jersey City"), new DoubleWritable(465.04)));
		
		
		List<Pair<Text, DoubleWritable>> resultado = mapDriver.getExpectedOutputs();
		
		System.out.println("Resultado: " + resultado);
		
		mapDriver.runTest();
		
		
		
	}
	
	@Test
	public void testMapReduce() {
		String texto = "2012-01-02	12:12	Jersey City	Sporting Goods	465.04	MasterCard";
		mapReduceDriver.withInput(new LongWritable(1), new Text(texto));
		mapReduceDriver.withOutput(new Pair<Text,DoubleWritable>(new Text("Jersey City"), new DoubleWritable(465.04)));
		
		//mapReduceDriver.withOutput(new Text("Sporting Goods"), new DoubleWritable(465.04));
		
		
		List<Pair<Text, DoubleWritable>> resultado = mapReduceDriver.getExpectedOutputs();
		
		System.out.println("Resultado mapreduce: " + resultado);
		mapReduceDriver.runTest();
		
		
		
	}
	
	@Test
	public void testReduce() {
		List<DoubleWritable> values1 = new ArrayList<DoubleWritable>();
		values1.add(new DoubleWritable(100));
		values1.add(new DoubleWritable(10));
		values1.add(new DoubleWritable(70));
		
		
		reduceDriver.withInput(new Text("Sporting Goods"), values1);
		reduceDriver.addOutput(new Pair<Text,DoubleWritable>(new Text("Sporting Goods"), new DoubleWritable(100)));
		List<Pair<Text, DoubleWritable>> resultado = reduceDriver.getExpectedOutputs();
		
		System.out.println("Resultado reduce: " + resultado);
		
		reduceDriver.runTest();
				
		
		
	}

	

}
