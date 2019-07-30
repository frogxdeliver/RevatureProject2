package com.revature.test;

import java.util.Arrays;

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

public class P2MaleEmploymentTest {
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
		mapDriver.withInput(new LongWritable(1), new Text("\"World\",\"WLD\",\"Employment to population ratio, 15+, male (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"75.84177889\",\"75.74513727\",\"75.42870636\",\"75.17438779\",\"75.06315069\",\"74.81003333\",\"74.55600733\",\"74.1962422\",\"74.09547083\",\"73.9489970024712\",\"73.746108\",\"73.27740409\",\"73.07182312\",\"73.05315168\",\"73.0941333\",\"73.05764391\",\"73.11939078\",\"72.83799946\",\"72.13390098\",\"72.084637\",\"72.01406328\",\"71.98991782\",\"71.92341082\",\"71.94852439\",\"72.02755423\",\"72.0073062034121\""));
		
		
		//mapDriver.withInput(new LongWritable(1), new Text("\"TestCountry\",\"AFG\",\"Educational attainment, at least Bachelor's or equivalent, population 25+, female (%) (cumulative)\",\"SE.TER.CUAT.BA.FE.ZS\", \"\""));

		mapDriver.withOutput(new Text("\"World\""), new Text("\"73.9489970024712\",\"72.0073062034121\""));
		

		mapDriver.runTest();
	}

	@Test
	public void testReducer() {
		reduceDriver.withInput(new Text("\"World\""), Arrays.asList(new Text("\"73.9489970024712\",\"72.0073062034121\""))); 
		
		
		reduceDriver.withOutput(new Text("World"), new Text("-2.6257%"));

		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {
		
		mapReduceDriver.withInput(new LongWritable(1), new Text("\"World\",\"WLD\",\"Employment to population ratio, 15+, male (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"75.84177889\",\"75.74513727\",\"75.42870636\",\"75.17438779\",\"75.06315069\",\"74.81003333\",\"74.55600733\",\"74.1962422\",\"74.09547083\",\"73.948997\",\"73.746108\",\"73.27740409\",\"73.07182312\",\"73.05315168\",\"73.0941333\",\"73.05764391\",\"73.11939078\",\"72.83799946\",\"72.13390098\",\"72.084637\",\"72.01406328\",\"71.98991782\",\"71.92341082\",\"71.94852439\",\"72.02755423\",\"72.0073062\""));
		
		mapReduceDriver.withOutput(new Text("World"), new Text("-2.6257%"));
		
		mapReduceDriver.runTest();

	}

	@After
	public void tearDown() {
		mapDriver = null; // garbage collection basically (empties the
							// mapDriver). It is redundant though
		reduceDriver = null;
	}

}
