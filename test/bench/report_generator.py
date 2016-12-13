import math
import matplotlib.pyplot as plt
from matplotlib.mlab import csv2rec
from matplotlib.cbook import get_sample_data
from matplotlib.ticker import Formatter, MultipleLocator
from os.path import basename, splitext

class TagFormatter(Formatter):
    def __init__(self, tags):
        self.tags = tags

    def __call__(self, x, pos=0):
        'Return the label for time x at position pos'
        ind = int(round(x))
        if ind >= len(self.tags) or ind < 0:
            return ''
        return str(self.tags[ind])

# These are the colors that will be used in the plot
color_sequence = ['#1f77b4', '#aec7e8', '#ff7f0e', '#ffbb78', '#2ca02c',
                  '#98df8a', '#d62728', '#ff9896', '#9467bd', '#c5b0d5',
                  '#8c564b', '#c49c94', '#e377c2', '#f7b6d2', '#7f7f7f',
                  '#c7c7c7', '#bcbd22', '#dbdb8d', '#17becf', '#9edae5']

def reportPreStep():
    return """
    <html>
    <body>
        <title>Realm Core Performance Metrics</title>
        <h1>Notes</h1>
        <div style="width:100%; text-align:center">"""

def reportPostStep():
    return """
        </div>
    </body>
</html>"""

def reportMidStep(name):
    return "<h1>" + name + "</h1>" + "<p>Details generated</p><img align=\"middle\" src=\"" + name + "\"/>"

def getThreshold(points):
    # assumes that data has 2 or more points
    # remove the last value from computation since we are testing it
    data = points[:-1]
    mean = float(sum(data)) / max(len(data), 1)
    variance = 0
    deviations = [ math.pow(x - mean, 2) for x in data ]
    variance = sum(deviations) / max(len(deviations), 1)
    std = math.sqrt(variance)
    threshold = mean + (2 * std)
    return threshold

def generateReport(outputDirectory, csvFiles):
    metrics = ['min', 'max', 'med', 'avg']

    report = reportPreStep()

    for index, fname in enumerate(csvFiles):
        bench_data = csv2rec(fname)

        print "generating graph: " + str(index) + "/" + str(len(csvFiles)) + " (" + fname + ")"
        formatter = TagFormatter(bench_data['tag'])

        fig, ax = plt.subplots()
        ax.xaxis.set_major_formatter(formatter)
        tick_spacing = 1
        ax.xaxis.set_major_locator(MultipleLocator(tick_spacing))

        plt.grid(True)
        for rank, column in enumerate(metrics):
            line, = plt.plot(bench_data[column], lw=2.5, color=color_sequence[rank])
            line.set_label(column)

        plt.legend()
        plt.xlabel('Build')
        plt.ylabel('Seconds')

        # rotate x axis labels for readability
        fig.autofmt_xdate()

        threshold = getThreshold(bench_data['avg'])
        plt.axhline(y=threshold, color='r')

        title = splitext(basename(fname))[0]
        plt.title(title, fontsize=18, ha='center')
        imgName = str(title) + '.png'
        plt.savefig(outputDirectory + imgName)
        # refresh axis and don't store these in memory
        plt.close(fig)

        report += reportMidStep(imgName)

    report += reportPostStep()

    with open(outputDirectory + str('report.html'), 'w+') as reportFile:
        reportFile.write(report)

