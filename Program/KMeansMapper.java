package de.jungblut.clustering.mapreduce;
import java.io.IOException;
import java.util.LinkedList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;

import de.jungblut.clustering.model.ClusterCenter;
import de.jungblut.clustering.model.DistanceMeasurer;
import de.jungblut.clustering.model.Vector;

// first iteration, k-random centers, in every follow-up iteration we have new calculated centers
public class KMeansMapper extends
		Mapper<ClusterCenter, Vector, ClusterCenter, Vector> {

	LinkedList<ClusterCenter> centers = new LinkedList<ClusterCenter>();

	@SuppressWarnings("deprecation")
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();
		Path centroids = new Path(conf.get("centroid.path"));
		FileSystem fs = FileSystem.get(conf);

		SequenceFile.Reader reader = new SequenceFile.Reader(fs, centroids,	conf);
		ClusterCenter key = new ClusterCenter();
		IntWritable value = new IntWritable();
		while (reader.next(key, value)) {
			key.getCenter().StockName="undone";
			centers.add(new ClusterCenter(key));
		}
		reader.close();
	}

	@Override
	protected void map(ClusterCenter key, Vector value, Context context)
			throws IOException, InterruptedException {

		ClusterCenter nearest = null;
		double nearestDistance = Double.MAX_VALUE;
		for (ClusterCenter c : centers) 
		{
			double dist = DistanceMeasurer.measureDistance(c, value);
			if (nearest == null) 
			{
				nearest = c;
				nearestDistance = dist;
			} else 
			{
				if (nearestDistance > dist)
				{
					nearest = c;
					nearestDistance = dist;
		     	}
			}	
		}
	nearest.getCenter().StockName="done";
	context.write(nearest, value);
	}
	
	protected void cleanup(Context context) throws IOException,
	InterruptedException {
     super.cleanup(context);
     for (ClusterCenter c : centers) 
    	 if(c.getCenter().StockName!="done")
    		 context.write(c,new Vector("",c.getCenter().vector.length));
	}
}
