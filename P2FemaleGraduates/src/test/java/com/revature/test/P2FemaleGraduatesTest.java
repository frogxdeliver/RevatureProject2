package com.revature.test;

import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.PercentageMapper;
import com.revature.reduce.PercentageReducer;

public class P2FemaleGraduatesTest {

	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;

	@Before
	public void setUp() {
		// Set up Mapper
		PercentageMapper mapper = new PercentageMapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(mapper);

		// Set up Reducer
		PercentageReducer reducer = new PercentageReducer();
		reduceDriver = new ReduceDriver<>();
		reduceDriver.setReducer(reducer);

		// Set up MapReducer
		mapReduceDriver = new MapReduceDriver<>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);

	}

	@Test
	public void testMapper() {
		mapDriver.withInput(new LongWritable(1), new Text("\"Afghanistan\",\"AFG\",\"Educational attainment, at least Bachelor's or equivalent, population 25+, female (%) (cumulative)\",\"SE.TER.CUAT.BA.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"23.401\",")); 
		//mapDriver.withInput(new LongWritable(1), new Text("\"TestCountry\",\"AFG\",\"Educational attainment, at least Bachelor's or equivalent, population 25+, female (%) (cumulative)\",\"SE.TER.CUAT.BA.FE.ZS\", \"\""));

		mapDriver.withOutput(new Text("\"Afghanistan\""), new Text("\"23.401\" CUAT 2016"));
		

		mapDriver.runTest();
	}

	@Test
	public void testReducer() {
		reduceDriver.withInput(new Text("\"Afghanistan\""), Arrays.asList(new Text("\"23.401\" CUAT 2016"))); 
		
		
		reduceDriver.withOutput(new Text("Afghanistan"), new Text("23.401% 2016"));

		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {
		
		mapReduceDriver.withInput(new LongWritable(1), new Text("\"Afghanistan\",\"AFG\",\"Educational attainment, at least Bachelor's or equivalent, population 25+, female (%) (cumulative)\",\"SE.TER.CUAT.BA.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"23.401\","));
		
		mapReduceDriver.withOutput(new Text("Afghanistan"), new Text("23.401% 2016"));
		
		mapReduceDriver.runTest();

	}

	@After
	public void tearDown() {
		mapDriver = null; // garbage collection basically (empties the
							// mapDriver). It is redundant though
		reduceDriver = null;
	}

}
