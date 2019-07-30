package com.revature.reduce;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class PercentageReducer extends
		Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text country, Iterable<Text> stats, Context context)
			throws IOException, InterruptedException {

		/*
		 * First we clean the country name by removing the quotation marks that are around the country name
		 * In the csv file, the country name has quotation marks around it so we want to remove those 
		 */

		String Country = country.toString();
		Country = Country.replaceAll("^\"|\"$", "");

		double value = 0;
		int counter = 0;
		int HIATcounter = 0;
		String year="";

		/*
		 * On every line in our mapper output, a country name is used as the key and the statistic along with the type of statistic is the value (key-value pairs)
		 * First we split the statistic into an array using " " as the expression to split on
		 * This will create a String array of 2 strings, the first index holding the string for the statistic and the second index holding the description of the statistic
		 * First we look for the CUAT stat which is the cumulative stat. Our assumption is that if a country includes the cumulative stat for females who at least have a bachelors degree,
		 * then that stat will be sufficient for our purposes
		 * 
		 *  We've set up a counter so that if the CUAT stat is not blank for that country, then we set value equal to that stat and increment our counter 
		 *  and this will cause the other statistics to not be read for that country
		 *  
		 *  If the country does not have a CUAT stat, then we will look at the HIAT stats for that country
		 *  We add up these HIAT stats because these stats represent the highest attained level of education,
		 *  so the percentage of people who's highest attained degree is Master's or Doctorate would satisfy the condition
		 *  of having a bachelor's degree as well.
		 *  
		 *  Thus we add together the three HIAT stats in an effort to encompass all females who at least have a bachelor's degree
		 *  
		 */
		for (Text stat : stats) {
			String[] StatArray = stat.toString().split(" ");
			
			if (counter == 0) {
				if (StatArray[1].equals("CUAT")) {
					String cleaned = StatArray[0].replaceAll("^\"|\"$", "");
					if (!cleaned.equals("")) {
						value = Double.parseDouble(cleaned);
						counter = counter + 1;
						year=StatArray[2];
					}
				}
				if (StatArray[1].equals("HIAT.BA")) {
					String cleaned = StatArray[0].replaceAll("^\"|\"$", "");
					if (!(cleaned.equals(""))) {
						HIATcounter += 1;
						value = value + Double.parseDouble(cleaned);
						System.out.println(value);
						year=StatArray[2];
					}
				}
				if (StatArray[1].equals("HIAT.DO")) {
					String cleaned = StatArray[0].replaceAll("^\"|\"$", "");
					if (!(cleaned.equals(""))) {
						HIATcounter += 1;
						value = value + Double.parseDouble(cleaned);
						System.out.println(value);
						year=StatArray[2];
					}
				}
				if (StatArray[1].equals("HIAT.MS")) {
					String cleaned = StatArray[0].replaceAll("^\"|\"$", "");
					if (!(cleaned.equals(""))) {
						HIATcounter += 1;
						value = value + Double.parseDouble(cleaned);
						System.out.println(value);
						year=StatArray[2];
					}
					
				}
			}
		}
		/*
		 * So if a country has a CUAT stat (counter==1) or if a country has all three HIAT stats available (HIATCounter==3),
		 * then we don't need to change the value for those countries
		 * 
		 * However, if a country doesn't have a CUAT stat and also they don't have all 3 HIAT stats, we set the value to -1 to
		 * denote that these countries have insufficient data to draw a conclusion on the female graduation rate. A country needs to have
		 * all 3 HIAT stats for us to include them in our final results.
		 */
		if (counter == 1 || HIATcounter == 3) {
			
		} else
			value = -1;
		
		/*
		 * if the value is -1, meaning that country has insufficient data, we will display the message that says "this country does not provide sufficient data for this statistic"
		 *  
		 * the next step we did was filter for values under 30, because the business question is asking which countries have a female graduation rate under 30%
		 * 
		 * finally, we will print out all the countries that have a statistic for female graduation, only if the statistic is under 30.
		 */
		DecimalFormat df = new DecimalFormat("#.####");
		String Value = df.format(value);
		if(value==-1)
		{
			context.write(new Text (Country), new Text("This country does not provide sufficient data for this statistic"));
			
		}
		if(value<30 && value!=-1) {
			context.write(new Text (Country), new Text(Value+"% "+year));
		}
		
	}
}
