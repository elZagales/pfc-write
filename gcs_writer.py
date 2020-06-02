import base64
import time
import datetime
import os
import json
import hashlib
from stravalib import Client, unithelper
from google.cloud import datastore
from google.cloud import storage
from google.cloud import bigquery


class AthleteActivity(object):
    def __init__(self, params):
        self.athlete_hub_seq = hashlib.md5(str(params['athlete_id']).encode()).hexdigest()
        self.athlete_id = params['athlete_id']
        self.activity_hub_seq = hashlib.md5(str(params['activity_id']).encode()).hexdigest()
        self.activity_id = params['activity_id']
        self.athlete_activity_link_seq = hashlib.md5(
            (str(self.athlete_id) + str(self.activity_id)).encode()).hexdigest()
        self.activity_type = params.get('type', 'None')
        self.activity_name = params.get('name', None)
        self.distance_m = params.get('distance', None)
        self.distance_mi = params.get('distance_mi', None)
        self.elapsed_time_s = params.get('elapsed_time_s', None)
        self.start_date = params.get('start_date', None)
        self.average_speed_m = params.get('average_speed', None)
        self.average_speed_mi = params.get('average_speed_mi', None)
        self.max_speed_m = params.get('max_speed', None)
        self.max_speed_mi = params.get('max_speed_mi', None)
        self.load_date = params.get('load_date', None)
        self.record_source = 'Strava'
        self.deleted_ind = False

        self.activity_description = params.get('description', None)
        self.moving_time_s = params.get('moving_time_s', None)
        self.total_elevation_gain_m = float(params.get('total_elevation_gain', None))
        self.elev_high_m = params.get('elev_high', None)
        self.elev_low_m = params.get('elev_low', None)
        self.trainer = params.get('trainer', None)
        self.commute = params.get('commute', None)
        self.manual = params.get('manual', None)
        self.private = params.get('private', None)
        self.flagged = params.get('flagged', None)
        self.workout_type = params.get('workout_type', None)
        self.kilojoules = params.get('kilojoules', None)
        self.average_watts = params.get('average_watts', None)
        self.device_watts = params.get('device_watts', None)
        self.max_watts = params.get('max_watts', None)
        self.weighted_average_watts = params.get('weighted_average_watts', None)
        self.calories = params.get('calories', None)

    def hub(self):
        hub_record = [{
            'activity_hub_seq': self.activity_hub_seq,
            'activity_id': self.activity_id,
            'hub_load_date': self.load_date,
            'record_source': self.record_source
        }]

        return hub_record

    def satellite(self):
        satellite_record = {
            'activity_hub_seq': self.activity_hub_seq,
            'sat_load_date': self.load_date,
            'activity_name': self.activity_name,
            'activity_description': self.activity_description,
            'distance_m': self.distance_m,
            'distance_mi': self.distance_mi,
            'elapsed_time_s': self.elapsed_time_s,
            'moving_time_s': self.moving_time_s,
            'activity_type': self.activity_type,
            'start_date': self.start_date,
            'average_speed_m': self.average_speed_m,
            'average_speed_mi': self.average_speed_mi,
            'max_speed_m': self.max_speed_m,
            'max_speed_mi': self.max_speed_mi,
            'total_elevation_gain_m': self.total_elevation_gain_m,
            'elev_high_m': self.elev_high_m,
            'elev_low_m': self.elev_low_m,
            'trainer': self.trainer,
            'commute': self.commute,
            'manual': self.manual,
            'private': self.private,
            'flagged': self.flagged,
            'workout_type': self.workout_type,
            'kilojoules': self.kilojoules,
            'average_watts': self.average_watts,
            'device_watts': self.device_watts,
            'max_watts': self.max_watts,
            'weighted_average_watts': self.weighted_average_watts,
            'calories': self.calories,
            'record_source': self.record_source,
            'delete_ind': self.deleted_ind
            # 'hash_diff': hashlib.md5((str(self.activity_name).strip() + str(self.distance_m)
            #                           + str(self.elapsed_time_s) + str(self.activity_type).strip() + str(self.start_date)
            #                           + str(self.average_speed_m) + str(self.max_speed_m)).encode()).hexdigest()
        }

        to_be_hashed = []
        ignore_list = ['activity_hub_seq', 'sat_load_date', 'delete_ind']
        for k in list(satellite_record):
            if k not in ignore_list:
                if satellite_record[k] is None:
                    satellite_record.pop(k)
                else:
                    to_be_hashed.append(str(satellite_record[k]).strip())

        hash_string = hashlib.md5(''.join(to_be_hashed).encode()).hexdigest()
        satellite_record.update(hash_diff=hash_string)
        return [satellite_record]

    def link(self):
        link_record = [{
            'athlete_activity_seq': self.athlete_activity_link_seq,
            'athlete_hub_seq': self.athlete_hub_seq,
            'athlete_id': self.athlete_id,
            'activity_hub_seq': self.activity_hub_seq,
            'activity_id': self.activity_id,
            'link_load_date': self.load_date,
            'record_source': self.record_source
        }]

        return link_record


def upload_blob(activity_dict, athlete, activity, event_time, aspect):
    destination_blob_name = '{}/{}-{}-{}'.format(athlete, activity, event_time, aspect)
    bucket_name = 'strava-activity-storage'
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(data=json.dumps(activity_dict), content_type='application/json')

    print('User Activity uploaded to {}.'.format(destination_blob_name))


def main(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    STRAVA_CLIENT_ID = os.environ.get('strava_client_id')
    STRAVA_CLIENT_SECRET = os.environ.get('strava_client_secret')

    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    pubsub_dict = json.loads(pubsub_message.replace("'", '"'))
    print(pubsub_dict)
    datastore_client = datastore.Client(project='strava-int')
    bq_client = bigquery.Client(project='strava-int')
    strava_client = Client()

    # event notification from strava
    aspect_type = pubsub_dict['aspect_type']
    object_id = pubsub_dict['object_id']
    owner_id = pubsub_dict['owner_id']
    object_type = pubsub_dict['object_type']
    event_time = pubsub_dict['event_time']
    event_datetime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(event_time))

    if object_type == 'activity':
        now = time.time()

        if aspect_type == 'delete':
            athlete_activity_dict = [{'activity_hub_seq': hashlib.md5(str(object_id).encode()).hexdigest()
                                     , 'sat_load_date': event_datetime
                                     , 'delete_ind': True}]
            # athlete_activity_obj = AthleteActivity(athlete_activity_dict)
            # sat_athlete_activity = athlete_activity_obj.satellite()
            sat_table_ref = bq_client.dataset('strava_datavault').table('activity_sat')
            bq_client.load_table_from_json(athlete_activity_dict, sat_table_ref)

        if aspect_type != 'delete':
            # stored athlete from datastore
            athlete_key = datastore_client.key('Athlete', owner_id)
            athlete = datastore_client.get(athlete_key)
            if now > athlete['expires_at']:
                access_token = strava_client.refresh_access_token(client_id=STRAVA_CLIENT_ID,
                                                                  client_secret=STRAVA_CLIENT_SECRET,
                                                                  refresh_token=athlete['refresh_token'])

                athlete.update(access_token)
                datastore_client.put(athlete)

            # create new client for authenticated athlete
            athlete_client = Client(access_token=athlete['access_token'])
            activity = athlete_client.get_activity(object_id)
            activity_dict = activity.to_dict()
            supplement = {'athlete_id': owner_id,
                          'activity_id': object_id,
                          'load_date': event_datetime}
            activity_dict.update(supplement)

            # GCS Storage
            upload_blob(activity_dict, owner_id, object_id, event_time, aspect_type)

            converted_units = {
                'distance_mi': unithelper.mile(getattr(activity, 'distance', None)).get_num(),
                'average_speed_mi': unithelper.mph(getattr(activity, 'average_speed', None)).get_num(),
                'max_speed_mi': unithelper.mph(getattr(activity, 'max_speed', None)).get_num(),
                'elapsed_time_s': int(unithelper.timedelta_to_seconds(getattr(activity, 'elapsed_time', None))),
                'moving_time_s': int(unithelper.timedelta_to_seconds(getattr(activity, 'moving_time', None)))
            }

            activity_dict.update(converted_units)

            athlete_activity_obj = AthleteActivity(activity_dict)
            sat_athlete_activity = athlete_activity_obj.satellite()
            print(sat_athlete_activity)
            # BQ insert
            sat_table_ref = bq_client.dataset('strava_datavault').table('activity_sat')
            bq_client.load_table_from_json(sat_athlete_activity, sat_table_ref)
            if aspect_type == 'create':
                link_athlete_activity = athlete_activity_obj.link()
                link_table_ref = bq_client.dataset('strava_datavault').table('athlete_activity_link')
                bq_client.load_table_from_json(link_athlete_activity, link_table_ref)

                hub_activity = athlete_activity_obj.hub()
                hub_table_ref = bq_client.dataset('strava_datavault').table('activity_hub')
                bq_client.load_table_from_json(hub_activity, hub_table_ref)
