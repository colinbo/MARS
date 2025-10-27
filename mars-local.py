#!/usr/bin/env python3
import apache_beam as beam

def processline(line):
    yield line

def run():
    argv = [
    ]

    input = 'sample/*.csv'
    output = 'output/output'
    
    with beam.Pipeline(argv=argv) as p:
      (p
       | 'Read Files' >> beam.io.ReadFromText(input)
       | 'Process Lines' >> beam.FlatMap(processline)
       | 'Write Output' >> beam.io.WriteToText(output)
      )

    p.run()

if __name__ == '__main__':
    run()
