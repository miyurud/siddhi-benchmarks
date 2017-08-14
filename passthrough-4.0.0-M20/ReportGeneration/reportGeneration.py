from pylatex import Document, Section, Subsection, Command, Math, TikZ, Axis, \
    Plot, Figure, LongTabu, Tabu, Head, PageStyle
from pylatex.utils import italic, NoEscape, bold
import os
import csv
import numpy as np
import matplotlib.pyplot as plt

# get the absolute path of the testing directory instead of hard coding
# split it by '/'
# grab the wanted part
# In this scenario absolute path is '/home/gwthamy/projects/streamperf/git/siddhi-benchmarks/filter-4.0.0-M20/ReportGeneration'

absolute_path = [os.path.abspath(name) for name in os.listdir(".") if os.path.isdir(name)]

for line in absolute_path:
    path = line.split("/")[7]  # to grab filter version

dir = '/home/gwthamy/projects/streamperf/git/siddhi-benchmarks/' + path + '/filtered-results-' + path
# directory to save image
throughput_image_dir = "/var/tmp/" + path + "-throughputChart.png"
latency_image_dir = "/var/tmp/" + path + "-latencyChart.png"

# array to save throughput total
throughput = []
#array to save latency total
latency = []

# to calculate over all throughput and latency of this version
final_throughput_avg = 0
final_latency_avg = 0


# loop through each csv file in the specific folder
def get_all_files(directory):
    dir_list = os.listdir(directory)
    csv_files = []
    for e in dir_list:
        if e.endswith('.csv'):
            csv_files.append(e)
    return csv_files


def sum_from_csv(csv_file):
    cr = open(csv_file, 'r')
    # cr.next()
    file_content = cr.readlines()

    # initialize throughput total as zero
    throughput_total = 0
    # initialize total latency as zero
    latency_total = 0
    # array to save throughput in every iteration
    throughput_dataset = []
    # array to save latency in every iteration
    latency_dataset = []

    for line in file_content:
        line = line.strip()
        throughput_data = line.split(",")[1]
        float_throughput_data = float(throughput_data)
        throughput_total += float_throughput_data
        throughput_dataset.append(float_throughput_data)

        latency_data = line.split(",")[4]
        float_latency_data = float(latency_data)
        latency_total += float_latency_data
        latency_dataset.append(float_latency_data)

    # to calculate number of dataset
    throughput_dataset_length = len(throughput_dataset)
    throughput_average = throughput_total / throughput_dataset_length
    million_throughput_average = throughput_average / 1000000
    throughput.append(million_throughput_average)

    # to calculate number of dataset
    latency_dataset_length = len(latency_dataset)
    # convert it into microseconds
    latency_average = (latency_total / latency_dataset_length) * 1000
    latency.append(latency_average)

    print "Throughput-total is", throughput_total
    print "Average is", million_throughput_average
    print "latency average", latency_average


for each in get_all_files(dir):
    sum_from_csv(os.path.join(dir, each))

# count the number of files in the specific folder
no_of_file = []
for x in range(0, len(throughput)):
    no_of_file.append(x)

# To calculate over all average throughput of this verision
final_throughput_total = 0
for avg in throughput:
    final_throughput_total += avg
final_throughput_avg = final_throughput_total / len(throughput)
print final_throughput_avg

# To calculate over all average of latency of this version
final_latency_total = 0
for lat_avg in latency:
    final_latency_total += lat_avg

final_latency_avg = final_latency_total / len(latency)
print final_latency_avg


# Generating Barcharts (Average Throughput vs Siddhi version & Average Latency vs Siddhi version)
# Bar chart for Average Throughput vs Siddhi version

N = len(throughput)
print N
ind = np.arange(N)
width = 0.45
plt.figure(1)
rects1 = plt.bar(ind, throughput, width, color='red')
plt.xticks(ind + width / 2, (no_of_file))
plt.ylabel('Average Throughput(million events/second)')
plt.xlabel('runs')
plt.savefig(throughput_image_dir)

# Bar chart for Average latency vs Siddhi version
plt.figure(2)
rects2 = plt.bar(ind, latency, width, color='blue')
plt.xticks(ind + width / 2, (no_of_file))
plt.ylabel('Average Latency(microseconds)')
plt.xlabel('runs')
plt.savefig(latency_image_dir)


# Report Generation
if __name__ == '__main__':
    # basic document
    doc = Document()

    doc.preamble.append(Command('title', 'Report'))
    doc.preamble.append(Command('author', 'Stream Processor Performance Testing-' + path))

    doc.preamble.append(Command('date', NoEscape(r'\today')))
    doc.append(NoEscape(r'\maketitle'))
    doc.append(NoEscape(r'\newpage'))

# Generating summary results chart for each run
# throughput summary table and chart

with doc.create(
        Section('Average Throughput During 60-180 seconds time period since the start of the benchmarking experiment')):
    doc.append(NoEscape(r'\hfill \break'))
    fmt = "X[r] X[r]"
    with doc.create(Subsection('Summary Table')):
        with doc.create(LongTabu(fmt, spread="0pt")) as data_table:
            header_row = ['Experiment runs', 'Average Throughput(million events/second)']
            data_table.add_row(header_row, mapper=[bold])
            data_table.add_hline()
            data_table.add_empty_row()
            data_table.end_table_header()

            # Iterates through throughput array and no_file_array and append rescpective run and throughput in the row
            for x, y in np.c_[no_of_file, throughput]:
                row = [x, y]
                data_table.add_row(row)
    doc.append(NoEscape(r'\newpage'))
    # append graph into the pdf report
    with doc.create(Subsection("Graph")):
        with doc.create(Figure(position='h!')) as throughput_chart:
            throughput_chart.add_image(throughput_image_dir, width='450px')

    with doc.create(Subsection("results")):
        doc.append("Over all throughput average (million events/seconds) of")
        doc.append(NoEscape(r'\space'))
        doc.append(path)
        doc.append(NoEscape(r'\space'))
        doc.append("is")
        doc.append(NoEscape(r'\space'))
        doc.append(final_throughput_avg)

doc.append(NoEscape(r'\newpage'))
# latency summary table and chart for latency

with doc.create(
        Section('Average Latency During 60-180 seconds time period since the start of the benchmarking experiment')):
    doc.append(NoEscape(r'\hfill \break'))
    fmt = "X[r] X[r]"
    with doc.create(Subsection('Summary Table')):
        with doc.create(LongTabu(fmt, spread="0pt")) as data_table:
            header_row = ['Experiment runs', 'Average Latency (micro seconds)']
            data_table.add_row(header_row, mapper=[bold])
            data_table.add_hline()
            data_table.add_empty_row()
            data_table.end_table_header()

            # Iterates through throughput array and no_file_array and append rescpective run and throughput in the row
            for x, y in np.c_[no_of_file, latency]:
                row = [x, y]
                data_table.add_row(row)
    doc.append(NoEscape(r'\newpage'))
    # append graph into the pdf report
    with doc.create(Subsection("Graph")):
        with doc.create(Figure(position='h!')) as throughput_chart:
            throughput_chart.add_image(latency_image_dir, width='450px')
    with doc.create(Subsection("results")):
        doc.append("Over all latency average of")
        doc.append(NoEscape(r'\space'))
        doc.append(path)
        doc.append(NoEscape(r'\space'))
        doc.append("is")
        doc.append(NoEscape(r'\space'))
        doc.append(final_latency_avg)

doc.generate_pdf('Report', clean_tex=False)
