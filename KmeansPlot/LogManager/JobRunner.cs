using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Hadoop_SSH;
using System.IO;
using System.IO.Compression;
using System.Diagnostics;
using System.Net;
using LogManager;

namespace Hadoop_SSH
{
    public class JobRunner
    {
        private static string HomePath = @"E:\Dropbox\Dropbox\Colman\Big Data";
        public void Run(int numStocks,int days, int Kcenters, string filter)
        {
            if(Directory.Exists(HomePath + @"\input"))
            Directory.Delete(HomePath + @"\input", true);
            Directory.CreateDirectory(HomePath + @"\input");
            //check symbol and download data
            readSymbolsAndDownload(numStocks, days);
            //make zips
            if (File.Exists(HomePath + @"\input.zip"))
                File.Delete(HomePath + @"\input.zip");
            ZipFile.CreateFromDirectory(HomePath + @"\input", HomePath + @"\input.zip");
            if (File.Exists(HomePath + @"\program.zip"))
                File.Delete(HomePath + @"\program.zip");
            if (File.Exists(HomePath + @"\output.txt"))
                File.Delete(HomePath + @"\output.txt");
            ZipFile.CreateFromDirectory(HomePath + @"\Program", HomePath + @"\program.zip");
            //Connect to cloudera
            SSHManager con = new SSHManager("10.0.0.128", "cloudera", "cloudera");
            //local commands
            Console.WriteLine("Running local commands");
            con.RunCommand("rm -rf /home/cloudera/input");
            con.RunCommand("rm -rf /home/cloudera/clustering");
            con.RunCommand("rm -rf /home/cloudera/output.txt");
            con.RunCommand("mkdir /home/cloudera/input");
            con.RunCommand("mkdir /home/cloudera/clustering");
            con.Put(HomePath + @"\input.zip", "/home/cloudera/");
            con.Put(HomePath + @"\program.zip", "/home/cloudera/");
            con.RunCommand("cd /home/cloudera/ && unzip input.zip -d /home/cloudera/input");
            con.RunCommand("unzip program.zip -d /home/cloudera/clustering");

            //HDFS
            Console.WriteLine("Running HDFS commands");
            con.RunCommand("hadoop fs -rm -r /user/cloudera/input");
            con.RunCommand("hadoop fs -mkdir /user/cloudera/input");
            con.RunCommand("hadoop fs -put /home/cloudera/input/* /user/cloudera/clustering/input");
            con.RunCommand("hadoop fs -rm output.txt");
            con.RunCommand("javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop/client-0.20/* -d kmeans_classes /home/cloudera/clustering/*.java");
            con.RunCommand("jar -cvf kmeans.jar -C kmeans_classes/ .");
            con.RunCommand("/usr/bin/hadoop jar kmeans.jar de.jungblut.clustering.mapreduce.KMeansClusteringJob input output.txt "+filter+" "+Kcenters);
            con.RunCommand("hadoop fs -get output.txt /home/cloudera/");
            con.Get("/home/cloudera/output.txt", HomePath);
        }

        private void readSymbolsAndDownload(int numStocks, int days)
        {
            DateTime now = DateTime.Now;
            now = now.AddDays(-days);
            string from = "&a="+(now.Month -1).ToString();
            from+= "&b="+now.Day.ToString();
            from+= "&c="+now.Year.ToString();
            now = now.AddDays(days);
            string to = "&d="+(now.Month-1).ToString();
            to+= "&e=" + now.Day.ToString();
            to += "&f=" + now.Year.ToString();
            int numberSeen = 0;
            int counter = 0;
            string line;
            var rng = new Random();
            // Read the file and display it line by line.
            System.IO.StreamReader file = new System.IO.StreamReader(HomePath + @"\nasdaqlisted.txt");
            while ((line = file.ReadLine()) != null && counter < numStocks)
            {
                    string symbol = line.Split('|')[0];
                    WebClient client = new WebClient();
                    try
                    {
                        string req = "http://ichart.finance.yahoo.com/table.csv?s=" + symbol + from + to + "&g=d&ignore=.csv";
                        client.DownloadFile(req, HomePath + @"\input\" + symbol + ".csv");
                    }
                    catch (Exception ex) { }
                    counter++;
            }
            file.Close();
        }
    }
}
    
