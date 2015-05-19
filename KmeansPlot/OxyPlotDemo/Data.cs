using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace OxyPlotDemo
{
    public class Data
    {
        public static List<Cluster> clusters = new List<Cluster>();

        public static List<Measurement> GetData(int days)
        {
            var vectors = new List<Measurement>();
            var startDate = DateTime.Now.AddDays(-days);  
            int Cluster = 0, Day=0;
            string line;

            System.IO.StreamReader file = new System.IO.StreamReader(@"E:\Dropbox\Dropbox\Colman\Big Data\output.txt");
            while ((line = file.ReadLine()) != null)
            {
                Measurement vec = null;
                if (line.Contains("["))
                {
                    string[] tokens = line.Split(new char[] { ',', ']', '[' }, StringSplitOptions.RemoveEmptyEntries);
                    vec = new Measurement() { DetectorId = Cluster, DateTime = startDate.AddDays(Day++), Value = Double.Parse(tokens[3]) }; //parse by close
                    vectors.Add(vec);
                }
                else
               {
                   clusters.Add(new Cluster() { Id = Cluster, ClusterMembers = line });
                    Cluster++;
                    Day=0;
                    startDate = DateTime.Now.AddDays(-days);
                    file.ReadLine();
               }  
            }
            file.Close();
            vectors.Sort((m1, m2) => m1.DateTime.CompareTo(m2.DateTime));
            return vectors;
        }

        public static List<Cluster> GetClusters()
        {
            return clusters;
        }
    }



    public class Cluster
    {
        public int Id { get; set; }
        public String ClusterMembers { get; set; }
    }

    public class Measurement
    {
        public int DetectorId { get; set; }
        public double Value { get; set; }
        public DateTime DateTime { get; set; }
    }
}
