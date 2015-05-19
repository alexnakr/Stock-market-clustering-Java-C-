package de.jungblut.clustering.mapreduce;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.tools.GetConf;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import de.jungblut.clustering.model.TradeDay;
import de.jungblut.clustering.model.ClusterCenter;
import de.jungblut.clustering.model.Vector;

public class KMeansClusteringJob {

	private static final int NUM_COLUMNS = 4;
    private static Map<String,LinkedList<Vector>> canopMap = new HashMap<String,LinkedList<Vector>>();
	private static float NumOfPoints = 0;
    
	@SuppressWarnings({ "deprecation" })
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		int iteration = 1;
		Configuration conf = new Configuration();
		conf.set("num.iteration", iteration + "");
        
		Path in = new Path("files/clustering/import/data");
		Path outfile = new Path(args[1]);
		Path center = new Path("files/clustering/import/center/cen.seq");
		conf.set("centroid.path", center.toString());
		Path out = new Path("files/clustering/temp/depth_1");
		Job job = new Job(conf);
		
		job.setJobName("MainJob");
		job.setMapperClass(KMeansMapper.class);
		job.setReducerClass(KMeansReducer.class);
		job.setJarByClass(KMeansMapper.class);

		SequenceFileInputFormat.addInputPath(job, in);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(out)) 			fs.delete(out, true);
		if (fs.exists(center)) 			fs.delete(center, true);
		if (fs.exists(in))   			fs.delete(in, true);
		if (fs.exists(outfile))   		fs.delete(outfile, true);
		
		final SequenceFile.Writer dataWriter = SequenceFile.createWriter(fs,conf, in, ClusterCenter.class, Vector.class);
        //read input
		ReadInputToSeqence(new File(args[0]), dataWriter, args[2]);
        //Run canopy job
  		CanopyJob canpJob = new CanopyJob();
  		canpJob.runJob();
  	    //Convert Canopy Sequence file to List of Canopies
  		SetCanopiesToList();
  		//Run KMeans on every canopy
  		
  		FSDataOutputStream writer = fs.create(new Path(args[1]),true);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(writer));
        
  		RunCanopiesKMeans(bw,args[3]);
  		bw.close();
	}
	
	/*---------------------------------------------------------------------------*/
	@SuppressWarnings("deprecation")
	private static void RunCanopiesKMeans(BufferedWriter bw, String Kcenters) throws IOException, InterruptedException, ClassNotFoundException
	{
  		for(Entry<String, LinkedList<Vector>> e : canopMap.entrySet())   //for every canopy run KMeans
  		{	        
		    //delete previous input + centers 
	  	    int iteration = 1;
	  	    float canopSize=0;
	  	    
	  	    Configuration conf = new Configuration();	  	 
	  	    conf.set("num.iteration", iteration + "");
	  	    FileSystem fs = FileSystem.get(conf);
	  		Path in = new Path("files/clustering/import/data");
	  		Path out = new Path("files/clustering/temp/depth_1");
			Path center = new Path("files/clustering/import/center/cen.seq");
			
			conf.set("centroid.path", center.toString());
			Job job = new Job(conf);
			job.setJobName("KMeans Clustering Canopy"+e.getKey());
			job.setMapperClass(KMeansMapper.class);
			job.setReducerClass(KMeansReducer.class);
			job.setJarByClass(KMeansMapper.class);
			SequenceFileInputFormat.addInputPath(job, in);
			if (fs.exists(out)) 			fs.delete(out, true);
			if (fs.exists(center)) 			fs.delete(center, true);
			if (fs.exists(in))   			fs.delete(in, true);
	  		//write new canopy points to input + centers
			final SequenceFile.Writer centerWriter = SequenceFile.createWriter(fs,conf, center, ClusterCenter.class, IntWritable.class);
			final SequenceFile.Writer vectorWriter = SequenceFile.createWriter(fs,conf, in, ClusterCenter.class, Vector.class);
			for(Vector v : e.getValue())
			{
				vectorWriter.append(new ClusterCenter(new Vector((""),v.vector.length)),v);
				canopSize++;
			}
			vectorWriter.close();
			//choose centers
			float canopCentersToWrite = ((float)Integer.parseInt(Kcenters))*(canopSize/NumOfPoints);
			if(canopCentersToWrite < 1)
				NumOfPoints -= canopSize;
			else
			{
				for(int i=0; i < Math.round(canopCentersToWrite); i++)  // write centers to centers file
				{
					final IntWritable value = new IntWritable(0);
					int r = (int) (Math.random() * (canopSize));
					centerWriter.append(new ClusterCenter(e.getValue().get(r)), value);
				}		
				centerWriter.close();
		  		//run KMeans 
				SequenceFileOutputFormat.setOutputPath(job, out);
				job.setInputFormatClass(SequenceFileInputFormat.class);
				job.setOutputFormatClass(SequenceFileOutputFormat.class);
				job.setOutputKeyClass(ClusterCenter.class);
				job.setOutputValueClass(Vector.class);
				job.waitForCompletion(true);
		
				long counter = job.getCounters().findCounter(KMeansReducer.Counter.CONVERGED).getValue();
				iteration++;
				//Iterate until center converges
		        while (counter > 0) 
		  		{
		  			conf = new Configuration();
		  			conf.set("centroid.path", center.toString());
		  			conf.set("num.iteration", iteration + "");
		  			job = new Job(conf);
		  			job.setJobName("KMeans Clustering " + iteration);
		
		  			job.setMapperClass(KMeansMapper.class);
		  			job.setReducerClass(KMeansReducer.class);
		  			job.setJarByClass(KMeansMapper.class);
		
		  			in = new Path("files/clustering/temp/depth_" + (iteration - 1) + "/");
		  			out = new Path("files/clustering/temp/depth_" + iteration);
		
		  			SequenceFileInputFormat.addInputPath(job, in);
		  			if (fs.exists(out))
		  				fs.delete(out, true);
		
		  			SequenceFileOutputFormat.setOutputPath(job, out);
		  			job.setInputFormatClass(SequenceFileInputFormat.class);
		  			job.setOutputFormatClass(SequenceFileOutputFormat.class);
		  			job.setOutputKeyClass(ClusterCenter.class);
		  			job.setOutputValueClass(Vector.class);
		  			job.waitForCompletion(true);
		  			iteration++;
		  			counter = job.getCounters().findCounter(KMeansReducer.Counter.CONVERGED).getValue();
		  		}
               iteration--;
  	           Path path = new Path("files/clustering/temp/depth_"+iteration+"/part-r-00000");
	           SequenceFile.Reader reader = new SequenceFile.Reader(fs, path,conf);           
	           outputToProper(reader,bw);
	           fs.delete(new Path("files/clustering/temp/"),true);
			}
  		}
	 }
	
/*---------------------------------------------------------------------------*/
private static void outputToProper(SequenceFile.Reader reader,BufferedWriter bw) throws IOException
{
       Map<String,LinkedList<String>> centers = new HashMap<String,LinkedList<String>>();
	   ClusterCenter key = new ClusterCenter();
       Vector v = new Vector();
       
       while (reader.next(key, v)) 
       {      
    	   if(centers.containsKey(key.toString()))
    	   {
    		centers.get(key.toString()).add(v.StockName); 		
    	   }
    	   else
    	   {
    		   LinkedList<String> str= new LinkedList<String>();
    		   str.add(v.StockName);
    		   centers.put(key.toString(), str);
    	   }
       }
      for(Entry<String,LinkedList<String>> e : centers.entrySet())
      { 
    	   bw.write(e.getKey());
    	   for(int i=0; i<e.getValue().size();i++)
    		   bw.write(e.getValue().get(i)+',');
    	   bw.write("\n");
      }
      bw.flush();
      reader.close();	
}
/*---------------------------------------------------------------------------*/	
private static void SetCanopiesToList() throws IOException
{
	Path canopies = new Path("files/clustering/import/center/canopies.seq");
	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(conf);
	@SuppressWarnings("deprecation")
	SequenceFile.Reader reader = new SequenceFile.Reader(fs, canopies,	conf);
	ClusterCenter key = new ClusterCenter();
	Vector value = new Vector();
	LinkedList<Vector> lst;
	while (reader.next(key, value)) 
	{
		if(canopMap.containsKey(key.getCenter().StockName))
			canopMap.get(key.getCenter().StockName).add(new Vector(value));
		else
		{ 
			lst = new LinkedList<Vector>();
			lst.add(new Vector(value));
			canopMap.put(key.getCenter().StockName, lst);
		}
	NumOfPoints++;
	}
	reader.close();
}
/*---------------------------------------------------------------------------*/
private static void ReadInputToSeqence(File folder, SequenceFile.Writer dataWriter, String Filter) throws NumberFormatException, IOException
{
    ArrayList<Vector> vectors = new ArrayList<Vector>();
	ArrayList<TradeDay> tdays = new ArrayList<TradeDay>();
	BufferedReader br = null;
    double prevClose=0;
    String sCurrentLine;
      for (final File fileEntry : folder.listFiles())   //for all files
      {
    	  br = new BufferedReader(new FileReader(fileEntry));    
          for (int k=0;(sCurrentLine = br.readLine()) != null; k++)      //read lines
          {
        	if(k==1) prevClose = Float.parseFloat(sCurrentLine.split(",")[NUM_COLUMNS]);
            if(k>1)
            { 
              double[] features = new double[NUM_COLUMNS];
              for(int indx=0; indx<NUM_COLUMNS;++indx) 
              {
                    	if((Filter.contains("o") && indx==0) || (Filter.contains("h") && indx==1) || (Filter.contains("l")&& indx==2 || (Filter.contains("c")&& indx==3)))
                    	features[indx] = 100*(1-(Float.parseFloat(sCurrentLine.split(",")[indx+1])/prevClose));
                    	else features[indx] = 0;
              }
              prevClose = Float.parseFloat(sCurrentLine.split(",")[NUM_COLUMNS]);
              TradeDay day = new TradeDay(features); 
              tdays.add(day);}
          }  
          Vector vec = new Vector(fileEntry.getName().substring(0, fileEntry.getName().indexOf('.')),tdays.size());
          vec.setVector(tdays);
          vectors.add(vec);
          dataWriter.append(new ClusterCenter(new Vector((""),tdays.size())),vec);
          tdays.clear();
      }
   dataWriter.close(); 
   vectors.clear();
}
}