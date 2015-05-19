using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using OxyPlot;
using OxyPlot.Axes;
using OxyPlot.Series;
using OxyPlotDemo.Annotations;

namespace OxyPlotDemo.ViewModels
{
    public class MainWindowModel 
    {
        private PlotModel plotModel;

        public int Days { get; set; }
        public PlotModel PlotModel
        {
            get { return plotModel; }
            set { plotModel = value; }
        }

        public MainWindowModel(int days)
        {
            this.Days = days;
            PlotModel = new PlotModel();
            SetUpModel();
            LoadData();
        }

        private void SetUpModel()
        {
            PlotModel.LegendTitle = "Clusters";
            PlotModel.LegendOrientation = LegendOrientation.Horizontal;
            PlotModel.LegendPlacement = LegendPlacement.Outside;
            PlotModel.LegendPosition = LegendPosition.TopRight;
            PlotModel.LegendBackground = OxyColor.FromAColor(200, OxyColors.White);
            PlotModel.LegendBorder = OxyColors.Black;

            var dateAxis = new DateTimeAxis(AxisPosition.Bottom, "Date", "d/M") { MajorGridlineStyle = LineStyle.Solid, MinorGridlineStyle = LineStyle.Dot, IntervalLength = 80 };
            PlotModel.Axes.Add(dateAxis);
            var valueAxis = new LinearAxis(AxisPosition.Left, -10) { MajorGridlineStyle = LineStyle.Solid, MinorGridlineStyle = LineStyle.Dot, Title = "Percentage change" };
            PlotModel.Axes.Add(valueAxis);

        }

        private void LoadData()
        {
            List<Measurement> measurements = Data.GetData(this.Days);
            List<Cluster> clusters = Data.GetClusters();

            var dataPerDetector = measurements.GroupBy(m => m.DetectorId).OrderBy(m => m.Key).ToList();

            foreach (var data in dataPerDetector)
            {
                    string s = clusters[data.Key].ClusterMembers;
                    var lineSerie = new LineSeries
                    {
                        StrokeThickness = 2,
                        MarkerSize = 3,
                        CanTrackerInterpolatePoints = false,
                        Title = string.Format("Cluster {0} - {1}", data.Key, s),
                        Smooth = false,
                    };
                    data.ToList().ForEach(d => lineSerie.Points.Add(new DataPoint(DateTimeAxis.ToDouble(d.DateTime), d.Value)));
                    PlotModel.Series.Add(lineSerie);              
            }
        }
    }
}
