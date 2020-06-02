from stravalib import Client, unithelper
from google.cloud import bigquery
import hashlib
import time


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
        self.total_elevation_gain_m = params.get('total_elevation_gain', None)
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

def main():
    errors = [
  {
    "athlete_id": "53391315",
    "activity_id": "3379353540",
    "f0_": "1588334673"
  },
  {
    "athlete_id": "53391315",
    "activity_id": "3379210955",
    "f0_": "1588332874"
  },
  {
    "athlete_id": "9960733",
    "activity_id": "3379102433",
    "f0_": "1588331363"
  },
  {
    "athlete_id": "53391315",
    "activity_id": "3379115019",
    "f0_": "1588331282"
  },
  {
    "athlete_id": "13552557",
    "activity_id": "3379039915",
    "f0_": "1588330495"
  },
  {
    "athlete_id": "49284024",
    "activity_id": "3377504915",
    "f0_": "1588291949"
  }
]

    bq_client = bigquery.Client(project='strava-int')

    for row in errors:
        STRAVA_CLIENT_ID = 45100
        STRAVA_CLIENT_SECRET = 'bd30e0b7860f26798c95388ed211f195f4957e44'


        aspect_type = 'update'
        object_id = getattr(row, 'activity_id')
        owner_id = getattr(row, 'athlete_id')
        object_type = 'activity'
        event_datetime = time.strftime(getattr(row, 'link_load_date')

        strava_client = Client()


