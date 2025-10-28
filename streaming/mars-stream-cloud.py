#!/usr/bin/env python3
import apache_beam as beam
import os
import datetime

def processline(line):
    strline = str(line)
    parameters=strline.split(",")
    dateserial = parameters[0]
    ipaddr = parameters[1]
    action = parameters[2]
    srcacct = parameters[3]
    destacct = parameters[4]
    amount = float(parameters[5])
    name = parameters[6]
    outputrow = {'timestamp' : dateserial, 'ipaddr' : ipaddr, 'action' : action, 'srcacct' : srcacct, 'destacct' : destacct, 'amount' : amount, 'customername' : name}
    yield outputrow


def run():
    projectname = os.getenv('GOOGLE_CLOUD_PROJECT')
    bucketname = os.getenv('GOOGLE_CLOUD_PROJECT') + '-bucket'
    jobname = 'mars-job-' + datetime.datetime.now().strftime("%Y%m%d%H%M")
    region = 'us-central1'
    serviceaccount = os.getenv('SERVICE_ACCOUNT_EMAIL')
    print(serviceaccount)

    # https://cloud.google.com/dataflow/docs/reference/pipeline-options
    argv = [
      '--streaming',
      '--runner=DataflowRunner',
      '--project=' + projectname,
      '--job_name=' + jobname,
      '--region=' + region,
      '--staging_location=gs://' + bucketname + '/staging/',
      '--temp_location=gs://' + bucketname + '/temploc/',
      '--max_num_workers=2',
      '--machine_type=e2-standard-2',
      '--service_account_email=' + serviceaccount,
      '--save_main_session'
    ]

    subscription = "projects/" + projectname + "/subscriptions/activities-subscription"
    outputtable = projectname + ":mars.activities"
    
    print("Starting Beam Job - next step start the pipeline")
    with beam.Pipeline(argv=argv) as p:
        (p
         | 'Read Messages' >> beam.io.ReadFromPubSub(subscription=subscription)
         | 'Process Lines' >> beam.FlatMap(lambda line: processline(line))
         | 'Write Output' >> beam.io.WriteToBigQuery(outputtable)
         )
    p.run()


if __name__ == '__main__':
    run()
