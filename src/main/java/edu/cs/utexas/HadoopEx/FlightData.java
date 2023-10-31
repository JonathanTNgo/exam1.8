package edu.cs.utexas.HadoopEx;
import java.util.Comparator;


public class FlightData implements Comparable<FlightData> {
	public String airline;
	public int delay;
    public int day_of_week;

	public FlightData(String key, String text) {
        this.day_of_week = Integer.parseInt(key);
		String[] new_text = text.split(" ");
		this.delay = Integer.parseInt(new_text[1]);
		this.airline = new_text[0];
	}

    @Override
	public int compareTo(FlightData other) {
		return Integer.compare(this.delay, other.delay);
	}
}