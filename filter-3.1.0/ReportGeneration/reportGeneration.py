from pylatex import Document, Section, Subsection, Command, Math, TikZ, Axis, \
    Plot, Figure, LongTabu, Tabu, Head, PageStyle
from pylatex.utils import italic, NoEscape, bold
import os
import glob
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

file_counter=-1
output_files=[]

# array to save throughput total
throughput = 0


#array to save latency total
latency = 0

million_throughput_average=0

# loop through each csv file in the specific folder
#def get_all_files(directory):
    #dir_list = os.listdir(directory)
    #csv_files = []
    #for e in dir_list:
        #if e.endswith('.csv'):
	    
           # csv_files.append(e)
    #return csv_files
dir_list = os.listdir(dir)
csv_files=[]
for e in dir_list:
	if e.endswith('.csv'):
		file_counter+=1
print file_counter
	

my_glob = glob.iglob('/home/gwthamy/projects/streamperf/git/siddhi-benchmarks/filter-3.1.0/filtered-results-filter-3.1.0/*.csv')
newest = min(my_glob, key=os.path.getctime) if my_glob else None


cr = open(newest, 'r')
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
throughput=million_throughput_average

    # to calculate number of dataset
latency_dataset_length = len(latency_dataset)
    # convert it into microseconds
latency_average = (latency_total / latency_dataset_length) * 1000
latency=latency_average

print "Throughput-total is", throughput_total
print "Average is", million_throughput_average
print "latency average", latency_average
print "counter is", file_counter

#for each in get_all_files(dir):
#sum_from_csv(os.path.join(dir, each))

# count the number of files in the specific folder
no_of_file = []
for x in range(0,file_counter):
    no_of_file.append(x)




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
           # for x, y in np.c_[no_of_file, throughput]:
            row = [file_counter, throughput]
            data_table.add_row(row)
    doc.append(NoEscape(r'\newpage'))
    # append graph into the pdf report
   # with doc.create(Subsection("Graph")):
       # with doc.create(Figure(position='h!')) as throughput_chart:
            #throughput_chart.add_image(throughput_image_dir, width='450px')


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
            #for x, y in np.c_[no_of_file, latency]:
            row = [file_counter, latency]
            data_table.add_row(row)
    doc.append(NoEscape(r'\newpage'))
    # append graph into the pdf report
    

String="output"

page=repr(file_counter)
doc.generate_pdf(String+page, clean_tex=False)
