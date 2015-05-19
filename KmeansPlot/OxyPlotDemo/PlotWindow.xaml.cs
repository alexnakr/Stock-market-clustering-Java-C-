using System;
using System.Diagnostics;
using System.Windows;
using System.Windows.Media;

namespace OxyPlotDemo
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {

        private ViewModels.MainWindowModel viewModel;
        public int Days { get; set; }

        public MainWindow(int days)
        {
            this.Days = Days;
            viewModel = new ViewModels.MainWindowModel(this.Days);

            DataContext = viewModel;

            CompositionTarget.Rendering += CompositionTargetRendering;
            stopwatch.Start();

            InitializeComponent();
        }

        private System.Diagnostics.Stopwatch stopwatch = new Stopwatch();

        private void CompositionTargetRendering(object sender, EventArgs e)
        {
          
        }
    }
}
