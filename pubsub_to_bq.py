import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions,StandardOptions
from sys import argv
import argparse
import os
from concurrent import futures
from google.cloud import pubsub_v1
import re


credential_path = "cred.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
INPUT_SUBSCRIPTION = "projects/training-316807/subscriptions/subscriber-dataflow"

class CustomParsing(beam.DoFn):
    def to_runner_api_parameter(self, unused_context):
        return "beam:transforms:custom_parsing:custom_v0", None

    def process(self, element: bytes, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam):
        data = element.decode("utf-8")
        data = str(data)
        print('element',str(element))
        values = re.split(",", re.sub('\r\n', '', re.sub('"', '',data)))
        row = dict(zip(('id', 'brand', 'made_in','price','name'),values))
        yield row

class print_success():
    def s(s):
        print('success',s)
        return 'success'

def run():
    # data_ingestion = dataingestion()
    print_s = print_success()
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_subscription",
        help='Input PubSub subscription of the form"',
        default=INPUT_SUBSCRIPTION,
    )
    known_args, pipeline_args = parser.parse_known_args()
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True
    with beam.Pipeline(options=pipeline_options) as p:
        (p | "ReadFromPubSub" >> beam.io.gcp.pubsub.ReadFromPubSub(subscription="projects/training-316807/subscriptions/subscriber-dataflow", timestamp_attribute=None)
    |'parsing the message' >> beam.ParDo(CustomParsing())
    | 'WriteToBigQuery' >> beam.io.WriteToBigQuery('training-316807:pub_sub_dataset.pub_sub_new',write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
    )
    
if __name__ == "__main__":
    run()
