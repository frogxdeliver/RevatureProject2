package com.revature.reduce;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class PercentageReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text country, Iterable<Text> stats, Context context)
			throws IOException, InterruptedException {
		/*
		 * First we clean up the country name by removing the quotations that surround each country
		 */
		String Country = country.toString();
		Country = Country.replaceAll("^\"|\"$", "");
		/*
		 * We iterate through the Text Iterable that holds the statistic information.
		 * First we check to see if the country contains the relevant statistic,
		 * and if the country does have the statistic we create an array to hold the individual
		 * values of the year 2000 percentage and the year 2015 percentage.
		 * 
		 * We cast these values into double so we can perform the percent change calculation,
		 * which we calculated by subtracting the year 2015 percentage from the year 2000 percentage,
		 * then dividing that difference by the year 2000 percentage, and then multiplying by 100 (standard percent change formula).
		 * 
		 * We print out the country name with the percentage change if the statistics were available for that country,
		 * and if the statistic is not available we print out the country with the message "No Data Available".
		 */

		for (Text stat : stats) {
			String Stat=stat.toString();
			if (!(Stat.equals("No Data Available"))) {
				String[] StatArray = Stat.split(",");
				StatArray[0]=StatArray[0].replaceAll("^\"|\"$", "");
				StatArray[1]=StatArray[1].replaceAll("^\"|\"$", "");
				System.out.println(StatArray[0]+" "+StatArray[1]);
				double FirstValue = Double.parseDouble(StatArray[0]);
				double LastValue = Double.parseDouble(StatArray[1]);
				double percentChange = ((LastValue - FirstValue) / FirstValue) * 100;
				System.out.println(percentChange);
		
		        DecimalFormat df = new DecimalFormat("#.####");
		        String PercentChange = df.format(percentChange);
				
				context.write(new Text(Country),
						new Text(PercentChange + "%"));

			} else {
				context.write(new Text(Country), new Text("No Data Available"));
			}

		}

	}

}
