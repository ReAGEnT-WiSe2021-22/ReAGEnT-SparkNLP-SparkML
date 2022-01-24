package utils

import org.jfree.chart.plot.PlotOrientation
import org.jfree.chart.{ChartFactory, ChartPanel, JFreeChart}
import org.jfree.data.xy.DefaultXYDataset

import javax.swing.{JFrame, WindowConstants}

/**
 * Helper class to visualize the training data and models
 *
 * @author Schander 572893
 */
object TrainingVisualizer {

  /**
   * Shows a line graph with sentiment values (y axix) depending on a specific date (x axis)
   *
   * @param dates      x values, dates should be downsized
   * @param sentiments y values, sentiment values
   * @param title      title
   * @return JFrame object, that should be disposed later with disposeFrame()
   */
  def plotData(dates: Array[Double], sentiments: Array[Double], title: String): JFrame = {
    val dataArray = Array.ofDim[Double](2, sentiments.length)

    dataArray(0) = dates // x values
    dataArray(1) = sentiments // y values

    val dataset = new DefaultXYDataset
    dataset.addSeries("Training Data", dataArray)
    val plotTitle = title
    val xaxis = "dates"
    val yaxis = "sentiments"
    val orientation = PlotOrientation.VERTICAL
    val show = false
    val toolTips = false
    val urls = false

    val chart: JFreeChart = ChartFactory.createXYLineChart(plotTitle, xaxis, yaxis,
      dataset, orientation, show, toolTips, urls)

    val frame: JFrame = new JFrame("Data")
    frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE)

    val chartPanel: ChartPanel = new ChartPanel(chart)
    frame.setContentPane(chartPanel)
    frame.pack()
    frame.setVisible(true)

    frame
  }

  /**
   * Disposes given JFrame-object
   * @param frame Open JFrame
   */
  def disposeFrame(frame: JFrame): Unit = {
    frame.setVisible(false)
    frame.dispose()
  }

}
