from collections import OrderedDict
import math
import matplotlib as mpl
# configure to use a backend that doesn't require a display since this
# will be run from docker. It must be configured before importing pyplot
mpl.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.transforms as mtransforms
from matplotlib.mlab import csv2rec
from matplotlib.cbook import get_sample_data
from matplotlib.ticker import Formatter, MultipleLocator
import numpy as np
import re
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


def writeReport(outputDirectory, summary, stats):
    html = """
        <html>
        <head>
            <style type=\"text/css\">
                h1.pass { background-color: lightgreen; }
                h1.fail { background-color: darksalmon; }
                h1.stat { background-color: lightblue; }
                a.pass { color: green; }
                a.fail { color: red; }
                a.stat { color: blue; }
            </style>
        </head>
        <body>
            <title>Realm Core Performance</title>
            <div style="width:100%; text-align:center">
            <h1>Realm Core Performance</h1>
            <p>These graphs show the relative performance of certain operations
               between versions. The purpose of this report is to allow
               reviewers to see if there have been any unintentional performace
               regressions in the code being reviewed. It would be meaningless
               and misleading to use the numbers shown here to compare to other
               databases.
             </p>
            <br> """

    # standard deviation summary graph
    summaryGraphName = makeSummaryGraph(outputDirectory, summary)
    html += ("<h1>Summary</h1>"
             "<p>This summary graph shows the performance of each benchmark "
             "as compared to the mean of all other runs on previous versions."
             "The red line indicates the threshold of 2 standard deviations "
             "from the mean. A benchmark is marked for further inspection if "
             "the PR under test takes longer than this threshold. The same "
             "line is shown on each of the individual graphs in the rest of "
             "this report.</p>")
    html += "<img align=\"middle\" id=\"summary\" src=\""
    html += summaryGraphName + "\"/>"

    # generate color coded link summary
    html += "<ol>"
    for title, values in summary.iteritems():
        html += "<li><a class=\"" + values['status'] + "\" "
        html += "href=#" + title + ">" + title + "</a></li>"
    for title, values in stats.iteritems():
        html += "<li><a class=\"stat\" href=#"
        html += title.replace(' ', '_') + ">" + title + "</a></li>"
    html += "</ol><br>"

    # generate each graph section
    for title, values in summary.iteritems():
        html += "<h1 class=\"" + values['status']
        html += "\" id=\"" + title + "\">" + title + "</h1>"
        html += "<p>Threshold:  "
        html += str(values['threshold']) + " (2 standard deviations)</p>"
        html += "<p>Last Value: " + str(values['last_value'])
        html += " (" + str(values['last_std']) + " standard deviations)</p>"
        html += "<img align=\"middle\" src=\"" + values['src'] + "\"/>"

    for title, values in stats.iteritems():
        html += "<h1 class=\"stat\" id=\""
        html += title.replace(' ', '_') + "\">" + title + "</h1>"
        html += "<img align=\"middle\" src=\"" + values['src'] + "\"/>"

    html += """
            </div>
        </body>
        </html>"""

    with open(outputDirectory + str('report.html'), 'w+') as reportFile:
        reportFile.write(html)


def autoscale_based_on(ax, lines):
    ax.dataLim = mtransforms.Bbox.unit()
    # call once with ignore=True to escape being
    # limited to the unit bounding box
    fxy = np.vstack(lines[0].get_data()).T
    ax.dataLim.update_from_data_xy(fxy, ignore=True)
    for line in lines:
        xy = np.vstack(line.get_data()).T
        ax.dataLim.update_from_data_xy(xy, ignore=False)
    ax.autoscale_view()


def getThreshold(points):
    # assumes that data has 2 or more points
    # remove the last value from computation since we are testing it
    data = points[:-1]
    mean = float(sum(data)) / max(len(data), 1)
    variance = 0
    deviations = [math.pow(x - mean, 2) for x in data]
    variance = sum(deviations) / max(len(deviations), 1)
    std = math.sqrt(variance)
    # we define the warninng threshold as 2 standard deviations from the mean
    threshold = mean + (2 * std)
    last_value = points[-1]
    # last_std is the distance of the last value (the one under test)
    # from the mean, in units of standard deviations
    last_std = (last_value - mean) / std
    return [threshold, last_value, last_std]


def makeSummaryGraph(outputDirectory, summary):
    summaryGraphName = "summary.png"

    ratios = {title: summary[title]['last_std'] for title in summary}
    ratios = OrderedDict(sorted(ratios.items(), reverse=True))

    # the y locations for the groups
    indices = range(len(ratios))
    width = 1
    widths = [width/2.0] * len(ratios)

    fig, ax = plt.subplots()
    rects1 = ax.barh(indices, ratios.values(), width, color='b')

    # add some text for labels, title and axes ticks
    ax.set_xlabel('Standard Deviation')
    ax.set_title('Standard Deviations From Mean')
    ax.set_yticks(indices)
    ax.set_yticklabels('')
    ax.set_yticks([x + (width/2.0) for x in indices], minor=True)
    ax.set_yticklabels(ratios.keys(), minor=True)
    ax.set_ylim([0, len(ratios)])
    ax.set_xlim([-4, 4])
    # 2 std threshold line
    plt.axvline(x=2, color='r')

    fig.set_size_inches(10, 16)
    plt.tight_layout()
    plt.savefig(outputDirectory + summaryGraphName)
    plt.close(fig)

    return summaryGraphName

# separate out into a list the semvar parts of the tag
# "v10.0.0-alpha.4" -> ['10', '0', '0', 'alpha', '4']
# separator characters are one of: "v", ".", "-"
semVerRegex = re.compile(r"[v\.\-]")
def splitTagSemVer(tag):
    return list(filter(None, semVerRegex.split(tag)))

# if the string is a number, return it. Otherwise convert
# the string to a numeric value that sorts alphabetically
# Assumes characters have a max value of 255.
# This function should produce values so that tags are sorted as follows:
# v0.9       -> 0, 9, 0
# v1.0-alpha -> 1, 0, -670576722593
# v1.0-beta  -> 1, 0, -2628935152
# v1.0       -> 1, 0, 0
# v1.0-10    -> 1, 0, 10
def numericValueOf(s):
    try:
        return int(s)
    except ValueError:
        accumulate = 0
        ts = s[:min(7, len(s))] # more than 7 can overflow 64 bits
        for (ndx, char) in enumerate(reversed(ts)):
            accumulate -= (255 - (ord(ts[ndx]))) * pow(255, ndx)
        return accumulate

orderingColumns = ['versionMajor', 'versionMinor', 'versionPatch', 'versionExtra', 'versionExtra2', 'versionExtra3']
def addVersionColumns(data):
    types = {'names': list(orderingColumns), 'formats': ['int32', 'int32', 'int32', 'int32', 'int32', 'int32']}
    for element in data.dtype.descr:
        types['names'].append(element[0])
        types['formats'].append(element[1])
    newStructure = np.dtype(types)
    space = np.zeros(data.shape, dtype=newStructure)
    for col in list(data.dtype.fields):
        space[col] = data[col]
    for row in space:
        try:
            splits = splitTagSemVer(row['tag'])
            row['versionMajor'] = numericValueOf(splits[0])
            row['versionMinor'] = numericValueOf(splits[1])
            row['versionPatch'] = numericValueOf(splits[2])
            if len(splits) >= 4:
                row['versionExtra'] = numericValueOf(splits[3])
            if len(splits) >= 5:
                row['versionExtra2'] = numericValueOf(splits[4])
            if len(splits) >= 6:
                row['versionExtra3'] = numericValueOf(splits[5])
        except Exception as e:
            print("parse error while processing semver of " + str(row['tag']) + ": " + str(e))
    return space

def generateStats(outputDirectory, statsfiles):
    summary = {}
    colors = ['yellow', 'indigo', 'orange', 'lightblue', 'green', 'violet',
              'orangered', 'gray', 'lightblue', 'limegreen', 'navy']
    for index, fname in enumerate(statsfiles):
        benchmarkData = csv2rec(fname, delimiter=',')
        benchmarkData = addVersionColumns(benchmarkData)
        benchmarkData = np.sort(benchmarkData, order=orderingColumns)
        print ("generating stats graph: " + str(index + 1) +
               "/" + str(len(statsfiles)) + " (" + fname + ")")
        formatter = TagFormatter(benchmarkData['tag'])
        fig, ax = plt.subplots()
        ax.xaxis.set_major_formatter(formatter)
        tick_spacing = 1
        ax.xaxis.set_major_locator(MultipleLocator(tick_spacing))
        plt.grid(True)
        exclusions = orderingColumns + ['sha', 'tag']
        lines = []
        for ndx, col in enumerate(benchmarkData.dtype.names):
            if col not in exclusions:
                line, = plt.plot(benchmarkData[col], lw=2.5,
                                 color=colors[ndx % len(colors)])
                line.set_label(col)
                lines.append(line)
        autoscale_based_on(ax, lines)
        plt.legend(loc='upper left', fancybox=True, framealpha=0.7)
        plt.xlabel('Build')
        plt.ylabel('')
        # rotate x axis labels for readability
        fig.autofmt_xdate()
        title = splitext(basename(fname))[0]
        plt.title(title, fontsize=18, ha='center')
        imgName = str(title) + '.png'
        fig.set_size_inches(12, 6)
        plt.tight_layout()
        plt.savefig(outputDirectory + imgName)
        # refresh axis and don't store these in memory
        plt.close(fig)
        summary[title] = {'title': title, 'src': imgName}
    return summary

def generateReport(outputDirectory, csvFiles, statsfiles):
    metrics = ['min', 'max', 'med', 'avg']
    colors = {'min': '#1f77b4', 'max': '#aec7e8', 'med': '#ff7f0e',
              'avg': '#ffbb78', 'threshold': '#ff1111'}

    summary = {}

    for index, fname in enumerate(csvFiles):
        benchmarkData = csv2rec(fname, delimiter=',')
        # csv files are ordered by commit timestamp which will sort properly most of the time
        # but not when comparing branches under concurrent development
        benchmarkData = addVersionColumns(benchmarkData)
        benchmarkData = np.sort(benchmarkData, order=orderingColumns)

        print ("generating graph: " + str(index + 1) + "/" +
               str(len(csvFiles)) + " (" + fname + ")")
        formatter = TagFormatter(benchmarkData['tag'])

        fig, ax = plt.subplots()
        ax.xaxis.set_major_formatter(formatter)
        tick_spacing = 1
        ax.xaxis.set_major_locator(MultipleLocator(tick_spacing))

        lines_to_scale_to = []
        plt.grid(True)
        for rank, column in enumerate(metrics):
            line, = plt.plot(benchmarkData[column], lw=2.5, color=colors[column])
            line.set_label(column)
            if column != "max":
                lines_to_scale_to.append(line)

        autoscale_based_on(ax, lines_to_scale_to)
        plt.legend(loc='upper left', fancybox=True, framealpha=0.7)
        plt.xlabel('Build')
        plt.ylabel('Seconds')
        # rotate x axis labels for readability
        fig.autofmt_xdate()

        threshold, last_value, last_std = getThreshold(benchmarkData['avg'])
        plt.axhline(y=threshold, color=colors['threshold'])

        title = splitext(basename(fname))[0]
        plt.title(title, fontsize=18, ha='center')
        imgName = str(title) + '.png'
        fig.set_size_inches(12, 6)
        plt.tight_layout()
        plt.savefig(outputDirectory + imgName)
        # refresh axis and don't store these in memory
        plt.close(fig)
        status = "fail" if last_value > threshold else "pass"
        summary[title] = {'title': title, 'src': imgName,
                          'threshold': threshold, 'last_value': last_value,
                          'last_std': last_std, 'status': status}

    stats = OrderedDict(sorted(generateStats(outputDirectory,
                                             statsfiles).items()))
    summary = OrderedDict(sorted(summary.items()))
    writeReport(outputDirectory, summary, stats)

