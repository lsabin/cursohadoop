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
	
	
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, IntWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, IntWritable> mapReduceDriver;
	
	
	@Before
	public void setup() {
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(new LogFileMapper());
		
		reduceDriver = new ReduceDriver<Text, Text, Text, IntWritable>();
		reduceDriver.setReducer(new LogFileReducer());
		
		/*
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>();
		mapReduceDriver.setMapper(new LogFileMapper());
		mapReduceDriver.setReducer(new LogFileReducer());
		*/
		
	}
	

	
	
	@Test
	public void testMapMes() {
		
		String lineaLog = "10.223.157.186 - - [15/Jul/2009:15:50:35 -0700] \"GET / HTTP/1.1\" 200 9157";
		
		mapDriver.withInput(new LongWritable(0L), new Text(lineaLog));
		mapDriver.addOutput(new Pair<Text,Text>(new Text("10.223.157.186"), new Text("Jul")));
		
		//List<Pair<Text, Text>> resultado = mapDriver.getExpectedOutputs();
		
		//(System.out.println("Resultado por mes: " + resultado);
		
		
		for (int c = 48; c < 123; c++ ) {
			String aChar = new Character((char)c).toString();
			int cara = c;
			
				if (c >= 48 && c <=57) {
					cara = c + 122 - 47;
				}
			
			  int indice = cara - 97; //97 es el caracter de a
			  int grupo = indice / 9; // 9 es es numero de letras por partitioner

			
			System.out.println(aChar + ":" + c + ", grupo: " + grupo);
			
			
			  

		}
		
		
		mapDriver.runTest();
	}
	

	

}
