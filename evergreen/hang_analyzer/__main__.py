from src.hang_analyzer import HangAnalyzer

from optparse import OptionParser

parser = OptionParser(description=__doc__)
parser.add_option(
    '-m', '--process-match', dest='process_match', choices=['contains', 'exact'],
    default='contains', help="Type of match for process names (-p & -g), specify 'contains', or"
    " 'exact'. Note that the process name match performs the following"
    " conversions: change all process names to lowecase, strip off the file"
    " extension, like '.exe' on Windows. Default is 'contains'.")
parser.add_option('-p', '--process-names', dest='process_names',
                  help='Comma separated list of process names to analyze')
parser.add_option('-g', '--go-process-names', dest='go_process_names',
                  help='Comma separated list of go process names to analyze')
parser.add_option(
    '-d', '--process-ids', dest='process_ids', default=None,
    help='Comma separated list of process ids (PID) to analyze, overrides -p &'
    ' -g')
parser.add_option('-c', '--dump-core', dest='dump_core', action="store_true", default=False,
                  help='Dump core file for each analyzed process')
parser.add_option('-s', '--max-core-dumps-size', dest='max_core_dumps_size', default=10000,
                  help='Maximum total size of core dumps to keep in megabytes')
parser.add_option(
    '-o', '--debugger-output', dest='debugger_output', action="append",
    choices=['file', 'stdout'], default=['stdout'],
    help="If 'stdout', then the debugger's output is written to the Python"
    " process's stdout. If 'file', then the debugger's output is written"
    " to a file named debugger_<process>_<pid>.log for each process it"
    " attaches to. This option can be specified multiple times on the"
    " command line to have the debugger's output written to multiple"
    " locations. By default, the debugger's output is written only to the"
    " Python process's stdout.")

(options, _) = parser.parse_args()

print("going to analyze")
analyzer = HangAnalyzer(options)
analyzer.execute()
