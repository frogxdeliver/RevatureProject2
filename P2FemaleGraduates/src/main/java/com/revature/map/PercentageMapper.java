package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;

import com.sun.tools.javac.util.List;

public class PercentageMapper extends Mapper<LongWritable, Text, Text, Text> {

	static int FirstCountry = 0;

//	private Logger log = Logger.getLogger(clazz)
	
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] Dataline = line.split(",");
		String Append = "";
		
		
		/*In the dataset, the first country that appears is Afghanistan. Before Afghanistan, the statistics
		 * were for regions around the world, not specific countries. So we set FirstCountry to 1 once the line with Afghanistan is read
		 */
		
		if (Dataline[0].equals("\"Afghanistan\"")) {
			FirstCountry = 1;
		}
		
		/*
		 * If we have reached Afghanistan in the data set, we will first look for the SE.TER.CUAT.BA.FE.ZS statistic
		 * This statistic represents the cumulative percentage of females who have at least attained a bachelor's degree
		 * 
		 * Furthermore, we were interested in the various statistics that show the highest level of education attained for females
		 * We included the statistics that gave information on the highest level attained being Bachelor's, Master's, or Doctorate
		 * This would encompass all the females who have at least attained a bachelor's degree
		 * We also figured out that the Indicator Code will be in the 6th index of the array that is created when we split the line using the delimiter (',').
		 * 
		 * We chose to not include the statistics that represent post-secondary education, because post-secondary education includes any type of degree/certification 
		 * that is attained after high school. So this could include associate or trade school degrees, and we explicitly are looking for the minimum degree attained
		 * to be a bachelor's degree.
		 */
		if (FirstCountry == 1) {
			String Country = Dataline[0];
			if (Dataline[6].equals( "\"SE.TER.CUAT.BA.FE.ZS\"")) {
				Append = "CUAT";
			}
			if (Dataline[6].equals( "\"SE.TER.HIAT.BA.FE.ZS\"")) {
				Append = "HIAT.BA";
			}
			if (Dataline[6].equals( "\"SE.TER.HIAT.DO.FE.ZS\"")) {
				Append = "HIAT.DO";
			}
			if (Dataline[6].equals( "\"SE.TER.HIAT.MS.FE.ZS\"")) {
				Append = "HIAT.MS";
			}
			/*
			 * We check if the current line that is being read is for a statistic that we are interested in
			 * We loop through the array that holds the content of the current DataLine, and we start at the 7th position
			 * because prior to the 7th position, we just have information to describe the statistic, not the actual statistics
			 * 
			 * We create a percentage variable to hold the most recent statistic available for each statistic
			 * Our assumption for the business question is that we are looking for countries with female graduation rate under 30% for the most recent year data was collected for that country
			 */
			int percentageYear=0;
			if (!(Append.equals(""))) {
				String percentage = "";
				for (int i = 7; i < Dataline.length; i++) {
					if (!(Dataline[i].equals("\"\""))) {
						percentage = Dataline[i];
						percentageYear=i;
					}
				}
				if(percentageYear==62)
				{
					percentageYear=2015;
				} else {
					int difference = 62-percentageYear;
					percentageYear=2015-difference;
				}
				
				/*
				 * the mapper will print every country and the values for the 4 statistics we have specified above(will print an empty string if that stat is not available for that country)
				 */
				context.write(new Text(Country), new Text(percentage+" "+Append+" "+percentageYear));
			}
		}



	}
}
