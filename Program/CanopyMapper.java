package de.jungblut.clustering.mapreduce;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.mapreduce.Mapper;
import de.jungblut.clustering.model.ClusterCenter;
import de.jungblut.clustering.model.DistanceMeasurer;
import de.jungblut.clustering.model.Vector;

public class CanopyMapper extends
		Mapper<ClusterCenter, Vector, ClusterCenter, Vector> {

	List<Vector> CanopyCenters = new LinkedList<Vector>();
	double dist=0;
	final double T1 = 60;
	final double T2 = 150;
	
	@Override
	protected void map(ClusterCenter key, Vector value, Context context)
			throws IOException, InterruptedException 
		{
		if(CanopyCenters.isEmpty())	
			CanopyCenters.add(new Vector(value));
		
		boolean ignore = false;
		for (Vector c : CanopyCenters) 
		{
			dist = DistanceMeasurer.measureDistance(c, value);
			if(dist<=T1) 
			{   
				ignore = true;      //inner radius -> delete
				break;
			}
			else if(dist<=T2)  
			{
                context.write(new ClusterCenter(c), value); //send point to proper center
                ignore = true;
                break;
			}
		}
		if(!ignore)
		{
		 CanopyCenters.add(new Vector(value));
		 context.write(new ClusterCenter(value), value);
		}
	}
}
