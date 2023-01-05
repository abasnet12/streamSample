package package2;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;


public class FirstDataStream_Project 
{

	public static void main(String[] args) throws Exception 
	{
		// TODO Auto-generated method stub
		//setup the stream execution environment
		final StreamExecutionEnvironment env1 = StreamExecutionEnvironment.getExecutionEnvironment();
		
		// checking input parameters
		final ParameterTool params1 = ParameterTool.fromArgs(args);
		
		//make parameters available in web interface
		env1.getConfig().setGlobalJobParameters(params1);
		
		DataStream<String> text1 = env1.socketTextStream("localhost", 6666);
		
		DataStream<Tuple2<String, Integer>> counts = text1.filter(new FilterFunction<String>() {
			
			public boolean filter(String value) throws Exception 
			{
				
				return value.startsWith("B");
				
			}
		})
			.map(new Tokenizer()).keyBy(0).sum(1);
		
		counts.print();
		env1.execute("Streamin WordCount");
		
	}// main
	
	public static final class Tokenizer implements MapFunction<String, Tuple2<String, Integer>>
	{
		public Tuple2<String, Integer> map (String value)
		{
			return new Tuple2<String, Integer>(value,1);
		}
	}//tatic final 

}// Data Stream
