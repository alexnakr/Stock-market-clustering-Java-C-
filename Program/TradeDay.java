package de.jungblut.clustering.model;

public class TradeDay 
{
	public double[] data;
	
	public TradeDay()
	{
	this.data = new double[]  {0,0,0,0};
	}
	
	public TradeDay(TradeDay day)
	{	
		this.data = new double[] {day.data[0],day.data[1],day.data[2],day.data[3]};
	}

	public TradeDay(double open, double high, double low, double close)
	{
		this.data = new double[] {open,high,low,close};
	}

	public TradeDay(double[] day) 
	{
		this.data = new double[] {day[0],day[1],day[2],day[3]};
	}
}
