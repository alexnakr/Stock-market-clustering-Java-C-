package de.jungblut.clustering.mapreduce;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import de.jungblut.clustering.model.ClusterCenter;
import de.jungblut.clustering.model.Vector;


public class CanopyJob {
	
	public CanopyJob() {}
	
	public void runJob() throws IOException,
	InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();

		Path in = new Path("files/clustering/import/data");
		Path canopies = new Path("files/clustering/import/center/canopies.seq");
		conf.set("canopies.path", canopies.toString());
		
		Path out = new Path("files/clustering/canopies");
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(out)) 			fs.delete(out, true);
		@SuppressWarnings("deprecation")
		Job job = new Job(conf);
		job.setJobName("Canopy Clustering");
		job.setMapperClass(CanopyMapper.class);
		job.setReducerClass(CanopyReducer.class);
		job.setJarByClass(CanopyMapper.class);

		SequenceFileInputFormat.addInputPath(job, in);
		SequenceFileOutputFormat.setOutputPath(job, out);
  		job.setInputFormatClass(SequenceFileInputFormat.class);
  		job.setOutputFormatClass(SequenceFileOutputFormat.class);
  		job.setOutputKeyClass(ClusterCenter.class);
  		job.setOutputValueClass(Vector.class);
  		job.waitForCompletion(true);
	}
}
