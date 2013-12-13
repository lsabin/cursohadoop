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


public class TestImageMapper {
	
	
	private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	
	
	@Before
	public void setup() {
		mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
		mapDriver.setMapper(new ImageCounterMapper());
		
	}

	
	
	@Test
	public void testMap() {
		
		String lineaLog1 = "10.82.30.199 - - [16/Jul/2009:03:00:26 -0700] \"GET /assets/img/dummy/films/thumbs/mum-and-dad.jpg HTTP/1.1\" 200 5148";
		String lineaLog2 = "10.82.30.199 - - [16/Jul/2009:03:00:26 -0700] \"GET /assets/img/dummy/films/thumbs/mum-and-dad.gif HTTP/1.1\" 200 5148";
		String lineaLog3 = "10.82.30.199 - - [16/Jul/2009:03:00:26 -0700] \"GET /assets/img/dummy/films/thumbs/mum-and-dad.png HTTP/1.1\" 200 5148";
		
		mapDriver.withInput(new LongWritable(0L), new Text(lineaLog1));
		mapDriver.withInput(new LongWritable(1L), new Text(lineaLog2));
		mapDriver.withInput(new LongWritable(2L), new Text(lineaLog3));
//		mapDriver.addOutput(new Pair<Text,Text>(new Text("10.223.157.186"), new Text("Jul")));
		
		List<Pair<Text, IntWritable>> resultado = mapDriver.getExpectedOutputs();
		System.out.println("Resultado por tipo fichero: " + resultado);

		
		
		mapDriver.runTest();
	}
	

	

}
