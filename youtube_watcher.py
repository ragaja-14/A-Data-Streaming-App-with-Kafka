#!/usr/bin/env python3
import logging
import sys
import requests
from config import config
import json
from pprint import pformat
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import SerializingProducer


def fetch_playlistvideos_perpage(my_api_key, playlist_id, next_page_token=None):
    response = requests.get("https://www.googleapis.com/youtube/v3/playlistItems",                                                  params={"key" : my_api_key, "playlistId": playlist_id , "part" : "contentDetails",
        "pageToken":next_page_token} )
    payload = json.loads(response.text)

    #logging.debug("response received {}".format(payload))
    return payload


def fetch_all_videos_inPlaylist(my_api_key, playlist_id,page_token=None):
    payload = fetch_playlistvideos_perpage(my_api_key,playlist_id,page_token)

    yield from payload["items"]

    next_page_token= payload.get("nextPageToken")
    if next_page_token is not None:
        yield from fetch_all_videos_inPlaylist(my_api_key,playlist_id,next_page_token)



def fetch_videos_page(google_api_key, video_id, page_token=None):
    response = requests.get("https://www.googleapis.com/youtube/v3/videos",
            params={"key":google_api_key,
                    "id": video_id,
                    "part": "snippet,statistics",
                    "pageToken" : page_token})
    payload = json.loads(response.text)

    return payload

def summarize_video(video):
    return {
            "video_id": video["id"],
            "title": video["snippet"]["title"],
            "views": int(video["statistics"].get("viewCount", 0)),
            "likes": int(video["statistics"].get("likeCount", 0)),
            "comments": int(video["statistics"].get("commentCount", 0)),
            }


def fetch_videos(google_api_key, video_id, page_token=None):
    payload = fetch_videos_page(google_api_key, video_id, page_token)
    yield from payload["items"]

    next_page_token= payload.get("nextPageToken")
    if next_page_token is not None:
        yield from fetch_videos(my_api_key,video_id,next_page_token)


def on_delivery(err, record):
    pass


def run():
    logging.info("START")

    schema_registry_client = SchemaRegistryClient(config["schema_registry"])
    youtube_videos_value_schema = schema_registry_client.get_latest_version("youtube_videos-value")

    kafka_config = config["kafka"] | {
                "key.serializer": StringSerializer(),
                    "value.serializer": AvroSerializer(
                                schema_registry_client,
                                        youtube_videos_value_schema.schema.schema_str,
                                            ),
                    }
    producer = SerializingProducer(kafka_config)

    my_api_key=config["google_api_key"]
    playlist_id=config["playlist_ID"]
    playlist_videos = fetch_all_videos_inPlaylist(my_api_key,playlist_id,None)
    
    for video_item in playlist_videos:
        video_id = video_item["contentDetails"]["videoId"]
        for video in fetch_videos(my_api_key, video_id):
            logging.info("received video ::::{}".format(pformat(summarize_video(video))))

            producer.produce(
                    topic="youtube_videos",
                    key=video_id,
                    value={
                        "TITLE": video["snippet"]["title"],
                        "VIEWS": int(video["statistics"].get("viewCount", 0)),
                        "LIKES": int(video["statistics"].get("likeCount", 0)),
                        "COMMENTS": int(video["statistics"].get("commentCount", 0)),
                        },
                    on_delivery=on_delivery,
                    )

            producer.flush()



    

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    sys.exit(run())

