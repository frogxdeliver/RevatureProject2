package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class PercentageMapper extends Mapper<LongWritable, Text, Text, Text> {

	static int FirstCountry = 0;

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		/*
		 * First we create an array of all the values in each line that is being read, using the delimiter (',') to split
		 */

		String line = value.toString();
		String[] Dataline = line.split(",");
		
		/*
		 * The first statistic we are interested in is for the world, and in the dataset the world statistics appear
		 * right before the first country which is Afghanistan.
		 * Thus, the data of interest we are looking for will begin once the lines starting with "World" are read
		 */
		

		if (Dataline[0].equals("\"World\"")) {
			FirstCountry = 1;
		}
		/*
		 * We determined that the statistic of interest is the employment to population ratio for males ages 15+
		 * We are using the ILO estimate due to the fact that more countries provide ILO estimate data for
		 * male employment compared to national estimate data
		 * 
		 * The business question is asking for change in percentage of male employment from year 2000, so we determined that 
		 * the statistic for year 2000 is located at index 46 for this particular indicator code.
		 * 
		 * We check to see if that country has a year 2000 statistic, and if they do, we store that statistic in FirstValue.
		 * If the year 2000 statistic is not available, we look for the year closest to 2000 which holds a statistic.
		 * 
		 * Through further analysis, we noted that every country that has an ILO estimate for male employment will have a statistic at year 2000.
		 * And also, the LastValue for every country that provides this statistic will be for year 2015.
		 * 
		 * 
		 */

		if (FirstCountry == 1) {
			String Country = Dataline[0];
			if (Dataline[5].equals("\"SL.EMP.TOTL.SP.MA.ZS\"")) {
				int counter = 0;
				String FirstValue = "";
				String LastValue = "";

				for (int i = 0; i < Dataline.length; i++) {
					if (i == 46) {
						System.out.println(Country + " " + Dataline[46]);
						counter = 1;
					}
					if (counter == 1 && !(Dataline[i].equals("\"\""))
							&& (FirstValue.equals(""))) {
						FirstValue = Dataline[i];
						
						
					}
					if (!(Dataline[i].equals("\"\""))) {
						LastValue = Dataline[i];
						
					}
				}
				/*
				 * We print out every country during the mapping phase, but if the ILO estimate for male employment is unavailable,
				 * we say No Data Available
				 * 
				 * If the country does have the statistic available, our mapper will output the country as the key and the value holds the year 2000 statistic
				 * and the year 2015 statistic.
				 */
				if(LastValue.equals("\"SL.EMP.TOTL.SP.MA.ZS\"")) {
					context.write(new Text(Country), new Text("No Data Available"));
				}else {
				context.write(new Text(Country), new Text(FirstValue + ","
						+ LastValue));
				}
			}

		}

	}
}
