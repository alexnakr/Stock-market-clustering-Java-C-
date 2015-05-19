package de.jungblut.clustering.model;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.io.WritableComparable;
import de.jungblut.clustering.model.TradeDay;


public class Vector implements WritableComparable<Vector> 
{
	private static final int NUM_COLUMNS = 4;
	public TradeDay[] vector;
	public String StockName;

	public Vector() {
		super();	
	}
	
	public Vector(String name, int days) {
		super();
		this.vector = new TradeDay[days];
		for(int i=0;i<days;i++)
			vector[i] = new TradeDay();
		this.StockName = name;
	}

	public Vector(Vector v) {
		super();
		this.StockName =new String(v.StockName);
		int l = v.vector.length;
		this.vector = new TradeDay[l];
		for(int i=0;i<l;i++)
			vector[i] = new TradeDay(v.vector[i]);    	
	}
	
	public void addDay(TradeDay day)
	{
		int l = vector.length;
		vector[l-1] = new TradeDay(day.data[0], day.data[1], day.data[2], day.data[3]);
	}

	public Vector(TradeDay[] days) {
		super();
		int l = days.length;
		this.vector = new TradeDay[l];
		for(int i=0; i<l; i++)
           System.arraycopy(days[i].data, 0, this.vector[i], 0, 4);
	}

    public Vector(TradeDay day, int days)
    {
		super();
        this.vector = new TradeDay[days];
        vector[0]= new TradeDay(day.data[0], day.data[1], day.data[2], day.data[3]);
    }
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(vector.length);
		for (int i = 0; i < vector.length; i++)
		   {
			for(int j = 0; j < NUM_COLUMNS ; j++)
			out.writeDouble(vector[i].data[j]);
		   }
		out.writeBytes(StockName+'\n');
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		int size = in.readInt();
		vector = new TradeDay[size];
		for (int i = 0; i < size; i++)
		{
			vector[i] = new TradeDay();
		    for(int j = 0; j < NUM_COLUMNS ; j++)
			vector[i].data[j] = in.readDouble();
		}
		StockName = in.readLine();
		StockName.length();
	}

	@Override
	public int compareTo(Vector o) {
        double c = 0;
		for (int i = 0; i < vector.length; i++) 
			for(int j = 0; j < NUM_COLUMNS ; j++)
			   c += this.vector[i].data[j] - o.vector[i].data[j]; 
			   
		if (c!= 0.0d)
			return (int)c;
		else
		    return 0;
		    
	}

	public TradeDay[] getVector() {
		return vector;
	}

	public void setVector(TradeDay[] vector) 
	{
		int size = vector.length;
		this.vector = new TradeDay[size];
		    for(int i=0; i < size ;i++) 
		    {   
		    	this.vector[i] = new TradeDay();
		    	for(int j=0;j<4;j++)
		    	this.vector[i].data[j] = vector[i].data[j];
		    }
	}

	public void setVector(ArrayList<TradeDay> list)
	{
		this.vector = new TradeDay[list.size()];
		for(int i=0; i< list.size();i++) 
			vector[i] = list.get(i);
	}
	
	@Override
	public String toString() {
		String toret = "";
		for(int i=0; i< vector.length;i++) 
			toret+=Arrays.toString(vector[i].data)+"\n" ;
		return toret;
	}
}
