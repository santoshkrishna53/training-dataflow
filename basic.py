import logging
import re
import apache_beam as beam
from apache_beam.io import ReadFromText
from google.cloud import pubsub_v1
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def run(argv=None, save_main_session=True):
  pipeline_options = PipelineOptions()
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  with beam.Pipeline(options=pipeline_options) as p:

    lines = (p | 'Read' >> ReadFromText('data.csv', skip_header_lines=1)
        | 'Write' >> WriteToText('output',file_name_suffix='.csv',num_shards=1))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()