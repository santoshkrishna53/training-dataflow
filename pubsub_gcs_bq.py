import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions,StandardOptions
from sys import argv
import argparse
import os
from concurrent import futures
from google.cloud import pubsub_v1
import re
import json

credential_path = "cred.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
INPUT_SUBSCRIPTION = "projects/training-316807/subscriptions/subscriber-dataflow"

class CustomParsing(beam.DoFn):
    def to_runner_api_parameter(self, unused_context):
        return "beam:transforms:custom_parsing:custom_v0", None

    def process(self, element: bytes, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam):
        data = element.decode("utf-8")
        data = str(data)
        data = 'gs://dataflow-pub-sub-training2/'+data
        print('filename',data)
        yield data

class dataingestion:
  def process(self, element):
    values = re.split(",", re.sub('\r\n', '', re.sub('"', '',element)))
    row = dict(zip(('id', 'brand', 'made_in','price','name'),values))
    print(row)
    return row

class readfile(beam.DoFn):
  def process(self, element):
    print('file inside' , str(element))
    test = beam.io.ReadFromText(element, skip_header_lines =1).side_inputs
    print('file content',test)
    # values = re.split(",", re.sub('\r\n', '', re.sub('"', '',string_input)))
    # row = dict(zip(('id', 'brand', 'made_in','price','name'),values))
    # print(row)
    yield element

def run():
    data_ingestion=dataingestion()
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_subscription",
        help='Input PubSub subscription of the form "projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."',
        default=INPUT_SUBSCRIPTION,
    )
    known_args, pipeline_args = parser.parse_known_args()
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        (p | "ReadFromPubSub" >> beam.io.gcp.pubsub.ReadFromPubSub(subscription="projects/training-316807/subscriptions/subscriber-dataflow", timestamp_attribute=None)
        |'parsing the message' >> beam.ParDo(CustomParsing())
        |'Reading data from gcs object' >> beam.io.ReadAllFromText()
        |'String To BigQuery Row' >> beam.Map(lambda s: data_ingestion.process(s))
        | 'WriteToBigQuery' >> beam.io.WriteToBigQuery('training-316807:pub_sub_dataset.pubsubbq',write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
        )
    
if __name__ == "__main__":
    run()
