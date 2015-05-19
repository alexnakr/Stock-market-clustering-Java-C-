package de.jungblut.clustering.mapreduce;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Reducer;
import de.jungblut.clustering.model.ClusterCenter;
import de.jungblut.clustering.model.Vector;

// receive all points for specific kanopy center
public class CanopyReducer extends
		Reducer<ClusterCenter, Vector, ClusterCenter, Vector> {

	Map<ClusterCenter,Vector> points = new HashMap<ClusterCenter,Vector>();
	
	@Override
	protected void reduce(ClusterCenter key, Iterable<Vector> values,
			Context context) throws IOException, InterruptedException {

        for(Vector v: values)
        	{
        	context.write(key, v);
    	    points.put(new ClusterCenter(key), new Vector(v));
    	    }
        }

	@SuppressWarnings("deprecation")
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);
		Configuration conf = context.getConfiguration();
		Path outPath = new Path(conf.get("canopies.path"));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(outPath, true);
		final SequenceFile.Writer out = SequenceFile.createWriter(fs,context.getConfiguration(), outPath, ClusterCenter.class,Vector.class);
		for (Entry<ClusterCenter,Vector> e : points.entrySet()) {
			out.append(e.getKey(), e.getValue());
		}
		out.close();
	}
}
