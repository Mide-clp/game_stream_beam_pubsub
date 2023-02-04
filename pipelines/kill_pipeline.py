"""
Pipeline to read game stream data, at the end of each game calculate the points for players, and point for teams
"""

import os
import apache_beam as beam
from apache_beam.transforms import window
from apache_beam.transforms.trigger import AccumulationMode, AfterProcessingTime, Repeatedly
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from dotenv import load_dotenv

load_dotenv()

service_account_dir = os.getenv("SERVICE_ACCOUNT")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_dir
# input subscription id
output_topic = os.getenv("OUTPUT_TOPIC")

# Replace 'my-output-subscription' with your output subscription id
input_subscription = os.getenv("INPUT_TOPIC")


class DoFilter(beam.DoFn):
    def process(self, element, filter_type):

        new_element = element[0].split(",")
        game_id = new_element[0]
        team_id = new_element[1]
        weapon_name = new_element[5]
        score = element[1]

        if filter_type == "team":
            team_name = new_element[2]
            return [(game_id + ", " + team_id + ", " + team_name + ", " + weapon_name, score)]

        elif filter_type == "player":
            player_id = new_element[4]
            player_name = new_element[4]
            return [(game_id + ", " + team_id + ", " + player_id + ", " + player_name + ", " + weapon_name, score)]


def create_player_weapon_pair(element):
    # print(element)
    game_id = element[0]
    player_id = element[1]
    player_name = element[2]
    team_id = element[3]
    team_name = element[4]
    weapon_name = element[5]
    weapon_rank = int(element[6])
    opponent_weapon_rank = int(element[13])
    rank_difference = weapon_rank - opponent_weapon_rank
    my_map = element[7]
    opponent_map = element[14]
    battle_time = int(element[15])
    score = 0

    # calculate point from battle rank
    if 20 >= battle_time <= 10:
        score += 4

    elif 30 >= battle_time <= 21:
        score += 3

    elif 40 >= battle_time <= 30:
        score += 2

    elif battle_time > 40:
        score += 1

    # calculate point from weapon rank
    if rank_difference > 6:
        score += 3

    elif rank_difference > 3:
        score += 2

    else:
        score += 1

    # calculate location distance
    if my_map != opponent_map:
        score += 3

    return (game_id + ", " + team_id + ", " + team_name + ", " + player_id + ", " + player_name + ", " + weapon_name,
            score)


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
            | "read only necessary columns needed" >> beam.Map(create_player_weapon_pair)
            # | "create key pair for grouping data" >> beam.Map(lambda data: (data[0], data[1:]))
            | "create session for games" >> beam.WindowInto(window.GlobalWindows(),
                                                            trigger=Repeatedly(AfterProcessingTime(10)),
                                                            accumulation_mode=AccumulationMode.ACCUMULATING)

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
