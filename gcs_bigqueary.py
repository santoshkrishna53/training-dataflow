
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from sys import argv
import re
import argparse

PROJECT_ID = 'training-316807'

class dataingestion:
  def parse_method(self, string_input):
    print('string' , string_input)
    values = re.split(",", re.sub('\r\n', '', re.sub('"', '',string_input)))
    row = dict(zip(('id', 'brand', 'made_in','price','name'),values))
    print(row)
    return row

if __name__ == '__main__':
    data_ingestion = dataingestion()
    parser = argparse.ArgumentParser()
    known_args = parser.parse_known_args(argv)
    p = beam.Pipeline(options=PipelineOptions())
    (p | 'ReadData' >> beam.io.ReadFromText('gs://dataflow-pub-sub-training2/data.csv', skip_header_lines =1)
    |'String To BigQuery Row' >> beam.Map(lambda s: data_ingestion.parse_method(s))
    | 'WriteToBigQuery' >> beam.io.WriteToBigQuery('springmltraining-316807:pub_sub_dataset.pub_sub',write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
    result = p.run()
