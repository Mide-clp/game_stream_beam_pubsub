"""
Pipeline to read game stream data, at the end of each game calculate the points for players, and point for teams
"""

import os
import apache_beam as beam
from apache_beam.transforms import window
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.trigger import AccumulationMode, AfterProcessingTime, Repeatedly
from dotenv import load_dotenv


service_account_dir = os.getenv("SERVICE_ACCOUNT")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_dir

output_topic = os.getenv("OUTPUT_TOPIC")

# input subscription id
input_subscription = os.getenv("INPUT_TOPIC")


class DoFilter(beam.DoFn):
    def process(self, element, filter_type):
        game_id = element[0]
        team_id = element[1]

        if filter_type == "team":
            team_name = element[2]
            return [(game_id + ", " + team_id + ", " + team_name, 1)]

        elif filter_type == "player":
            player_id = element[2]
            player_name = element[3]
            return [(game_id + ", " + team_id + ", " + player_id + ", " + player_name, 1)]


def create_player_pair(element):
    # print(element)
    game_id = element[0]
    team_id = element[1][2]
    team_name = element[1][3]
    player_id = element[1][1]
    player_name = element[1][1]

    return [game_id, team_id, team_name, player_id, player_name]


class TransformData(beam.PTransform):
    def expand(self, input_entry):
        a = (
                input_entry
                | " sum the total point gotten" >> beam.CombinePerKey(sum)
                | "format data" >> beam.Map(lambda data: data[0] + ", " + str(data[1]))
                | " format data to format for use" >> beam.Map(lambda data: str(data).encode())
                | "write to data  pubsub" >> beam.io.WriteToPubSub(topic=output_topic)
        )


options = PipelineOptions()
options.view_as(StandardOptions).streaming = True

with beam.Pipeline(options=options) as pipeline:
    print("connected")
    read_entry = (
            pipeline
            | " read file from pubsub for game data" >> beam.io.ReadFromPubSub(subscription=input_subscription)
            | "put data in the right format for use" >> beam.Map(lambda data: data.strip().decode().split(","))

    )

    apply_pre_transformation = (
            read_entry
            | "create key pair for grouping data" >> beam.Map(lambda data: (data[0], data[1:]))
            | "create session for games" >> beam.WindowInto(window.GlobalWindows(),
                                                            trigger=Repeatedly(AfterProcessingTime(10)),
                                                            accumulation_mode=AccumulationMode.ACCUMULATING)
            | "read only necessary columns needed" >> beam.Map(create_player_pair)

    )

    apply_team_transform = (
            apply_pre_transformation
            | "apply team transformation" >> beam.ParDo(DoFilter(), filter_type="team")
            | "transform data for teams" >> TransformData()

    )

    apply_player_transform = (
            apply_pre_transformation
            | "apply player transformation" >> beam.ParDo(DoFilter(), filter_type="player")
            | "transform data for players" >> TransformData()

    )

    pipeline.run().wait_until_finish()
