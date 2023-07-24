import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from sys import argv
import re
import argparse
import os
from concurrent import futures
from google.cloud import pubsub_v1
from concurrent import futures
from google.cloud import pubsub_v1
import os
import json
# credential_path = "cred.json"
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

project_id = "training-316807"
topic_id = "pub-sub-training"

class dataingestion:
  def parse_method(self, string_input):
    message = bytes(string_input, 'utf-8')
    print(message)
    return message
  
def pub(data):
  project_id="springmltraining-316807"
  topic_id = "pub-sub-training-new"
  client = pubsub_v1.PublisherClient()
  topic_path = client.topic_path(project_id, topic_id)
  print(type(data))
  message = bytes(str(data), 'utf-8')
  api_future = client.publish(topic_path, message)
  message_id = api_future.result()
  print(f"Published {data} to {topic_path}: {message_id}")

if __name__ == '__main__':
    data_ingestion = dataingestion()
    parser = argparse.ArgumentParser()
    known_args = parser.parse_known_args(argv)
    # p = beam.Pipeline(options=PipelineOptions())
    p = beam.Pipeline(argv=["--project springmltraining-316807", "--temp_location gs://dataflow-pub-sub-training2/temp","--staging_location gs://dataflow-pub-sub-training2/staging/"])
    (p | 'ReadData' >> beam.io.ReadFromText('gs://dataflow-pub-sub-training2/data.csv', skip_header_lines =1)
    |'Strimng to byteStriing' >> beam.Map(lambda s: data_ingestion.parse_method(s))
    |'publishing to pubsub' >> beam.Map(lambda s: pub(s))
    )
    result = p.run()
