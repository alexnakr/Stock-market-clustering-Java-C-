using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Shapes;
using Hadoop_SSH;

namespace OxyPlotDemo
{
    /// <summary>
    /// Interaction logic for Window1.xaml
    /// </summary>
    public partial class FirstWindow : Window
    {
        public int StocksToRun { get; set; }
        public int Kcenters { get; set; }
        public int Days { get; set; }
        public string FilterParameters { get; set; }
        public FirstWindow()
        {
            InitializeComponent();
        }

        private void Button_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                this.Kcenters = int.Parse(this.KcentersText.Text);
                this.Days = int.Parse(this.DaysText.Text);
                this.StocksToRun = int.Parse(this.Stocks.Text);
            }
            catch
            {
                MessageBox.Show("Error");
                this.Close();
            }

            if (open.IsChecked.Value) FilterParameters = "o";
            if(high.IsChecked.Value) FilterParameters += "h";
            if (low.IsChecked.Value) FilterParameters += "l";
            if (close.IsChecked.Value) FilterParameters += "c";

           //Run job
            JobRunner job = new JobRunner();
            job.Run(this.StocksToRun, this.Days, this.Kcenters, this.FilterParameters);

            MainWindow m = new MainWindow(this.Days);
            m.Show();
        }
    }
}
