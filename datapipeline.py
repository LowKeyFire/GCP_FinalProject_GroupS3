import math
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from google.cloud.bigquery import StandardSqlDataTypes as SQLType
from google.cloud.bigquery.schema import SchemaField

# Define the BigQuery schema for the congestion data
congestion_schema = parse_table_schema_from_json("""
{
  "fields": [
    {
      "name": "frame",
      "type": "INTEGER"
    },
    {
      "name": "lane_id",
      "type": "INTEGER"
    },
    {
      "name": "average_speed",
      "type": "FLOAT"
    }
  ]
}
""")

def process_data(element):
    # Split the input CSV line into a list of fields
    fields = element.split(',')

    # Extract relevant fields from the input data
    frame = int(fields[0])
    vehicle_id = int(fields[1])
    x = float(fields[2])
    y = float(fields[3])
    x_velocity = float(fields[6])
    y_velocity = float(fields[7])
    lane_id = int(fields[26])

    # Calculate the speed using the xVelocity and yVelocity fields
    speed = math.sqrt(x_velocity**2 + y_velocity**2)

    # Create a tuple (lane_id, speed) as the key-value pair for the current record
    lane_speed = (lane_id, speed)

    # The transformed element will have the same frame and vehicle_id, with the addition of lane_speed
    transformed_element = {
        'frame': frame,
        'vehicle_id': vehicle_id,
        'lane_speed': lane_speed
    }

    return transformed_element

#Predetermined speed to consider that traffic is congested
speed_threshold = 20

# Set up Dataflow pipeline options (Backup solution**)
#options = PipelineOptions()
#options.view_as(beam.options.pipeline_options.GoogleCloudOptions).project = 'trafficsystems3'
#options.view_as(beam.options.pipeline_options.StandardOptions).runner = 'DataflowRunner'

# Create the command-line argument parser
parser = argparse.ArgumentParser()

# Add the Dataflow command-line arguments
parser.add_argument('--runner', default='DirectRunner', help='Runner to use: DataflowRunner or DirectRunner')
parser.add_argument('--project', help='Google Cloud project ID')
parser.add_argument('--temp_location', help='GCS location for temporary files')
parser.add_argument('--region', help='Google Cloud region for the Dataflow job')

# Parse the command-line arguments
known_args, pipeline_args = parser.parse_known_args()

# Set the pipeline options
options = PipelineOptions(pipeline_args)
options.view_as(SetupOptions).save_main_session = True

# Create a Dataflow pipeline
with beam.Pipeline(options=options) as p:
    # Read all CSV files in the highd_dataset bucket
    input_data = p | 'Read from GCS' >> ReadFromText('gs://rawtrafficdata/highd_dataset/*.csv')
    # Remove the header line from the input data
    input_data_no_header = input_data | 'Remove Header' >> beam.Filter(lambda line: not line.startswith('frame,id,x,y,width,height,xVelocity,yVelocity,xAcceleration,yAcceleration,frontSightDistance,backSightDistance,dhw,thw,ttc,precedingXVelocity,precedingId,followingId,leftPrecedingId,leftAlongsideId,leftFollowingId,rightPrecedingId,rightAlongsideId,rightFollowingId,laneId'))
    processed_data = input_data_no_header | 'Process Data' >> beam.Map(process_data)
    
    # Group the data by frame and lane_id
    grouped_data = (processed_data
                    | 'Create Key' >> beam.Map(lambda x: ((x['frame'], x['lane_speed'][0]), x['lane_speed'][1]))
                    | 'Group By Key' >> beam.GroupByKey()
                   )

    # Calculate the average speed for each group
    congestion_data = (grouped_data
                      | 'Calculate Average Speed' >> beam.Map(lambda x: (x[0], sum(x[1]) / len(x[1])))
                      | 'Filter Congested Segments' >> beam.Filter(lambda x: x[1] < speed_threshold)
                     )

    # Write the results to GCS
    congestion_data | 'Write to GCS' >> WriteToText('gs://protrafficdata/highd_dataset/congestion_data.csv')

    # Write the congestion data to BigQuery
    congestion_data | 'Write to BigQuery' >> WriteToBigQuery(
        table='trafficsystems3.flaggedtraffic',
        schema=congestion_schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    )
