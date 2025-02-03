import requests
import duckdb
import logging
import re
import pprint
import pandas as pd
from datetime import datetime
from .utils import camel_to_snake
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

NOAA_URL = "https://forecast.weather.gov/MapClick.php?lat=%(lat)s&lon=%(lon)s&unit=0&lg=english&FcstType=json"

LOCATIONS = {'Paradise Mt Rainier': {"lat": 46.786, "lon": -121.735},
             'Stevens Pass': {"lat": 47.7462 , "lon": -121.0859},
             'Mt Baker': {"lat": 48.80, "lon": -121.63},
             'Snoqualmie Pass': {"lat": 47.4263, "lon": -121.4172},
             'Crystal Mountain': {"lat": 46.9291, "lon": -121.5009},
             'Mt St Helens': {"lat": 46.2406, "lon": -122.1459},
             'Mission Ridge': {"lat": 47.2769, "lon": -120.4204},
             'Hurricane Ridge': {"lat": 47.9722, "lon": -123.5220},
             'Alpental': {"lat": 47.4393, "lon": -121.445},
             'Sunrise Mt Rainier': {"lat": 46.9146, "lon": -121.642333333},
             'Camp Muir, Mt Rainier': {"lat": 46.8354, "lon": -121.7311},
             'Blewett Pass': {"lat": 47.3439, "lon": -120.5659},
            }

DB = 'weather.db'

class NOAA:
    def __init__(self):
        pass

    def get_data(self, location):
        url = NOAA_URL % {'lat': LOCATIONS[location]['lat'], 'lon': LOCATIONS[location]['lon']}
        try:
            r = requests.get(url)
        except Exception as e:
            logger.error("Error getting NOAA data: %s" % e)
            return None
        data = r.json()
        default_row = {"location_name": location}
        for k, v in data.items():
            if k != 'time' and k != 'data' and k != 'location' and k != 'currentobservation':
                default_row[camel_to_snake(k)] = v
        for k, v in data['location'].items():
            default_row[f"location_{camel_to_snake(k)}"] = v
            # time keys: layoutKey, startPeriodName, startValidTime, tempLabel
        
        forecast_rows = []
        # pprint.pprint(data)
        # exit(0)
        for i, v in enumerate(data['time']['startValidTime']):
            dt = datetime.fromisoformat(v)
            dt_utc = dt.astimezone(tz=datetime.utcnow().astimezone().tzinfo)
            forecast_row = {**default_row,
                            'date_time': dt_utc,
                            'weather': data['data']['weather'][i],
                            'iconLink': data['data']['iconLink'][i],
                            'text': data['data']['text'][i],
                            'precip_chance': data['data']['pop'][i],
                            'temp': data['data']['temperature'][i],
                            'temp_label': data['time']['tempLabel'][i],
                            }
            forecast_rows.append(forecast_row)

        current_observation = {**default_row}
        for obs in data['currentobservation']:
            current_observation[camel_to_snake(obs)] = data['currentobservation'][obs].replace('NA', '').replace('NULL', '')

        # for row in forecast_rows:
        #     print(row)

        return {'current_observation': current_observation, 'forecast': forecast_rows}
            
    def update_data(self):
        con = duckdb.connect(DB)
        noaa = NOAA()
        observations = []
        forecasts = []
        for loc in LOCATIONS:
            data = noaa.get_data(loc)
            observations.append(data['current_observation'])
            forecasts.extend(data['forecast'])

        # forecasts
        logger.debug(f"Forecasts: {len(forecasts)}")
        df = pd.DataFrame(forecasts)
        for col in df.columns:
            if re.match(r'.*(latit|longi|eleva|chance|temp$)', col):
                df[col] = pd.to_numeric(df[col], errors='coerce')
        df['date_time'] = pd.to_datetime(df['date_time']).dt.tz_localize(None)
        df['creation_date'] = pd.to_datetime(df['creation_date']).dt.tz_localize(None)
        df['created_at'] = datetime.utcnow()

        table_exists = False
        try:
            r = con.execute("select max(date_time) from noaa_forecast")
            row = r.fetchone()
            table_exists = True
            if row[0] is not None:
                latest_timestamp = row[0]
                logger.debug(f"Last timestamp: {latest_timestamp}")
        except Exception as e:
            e_str = str(e).replace('\n',' ')
            logger.error(f"Error getting last timestamp: {e_str}")


        if not table_exists:
            con.register('noaa_forecast_import', df)
            logger.info("Creating new noaa_forecast_import table with %s rows" % len(df))
            con.execute("CREATE TABLE noaa_forecast AS SELECT * FROM noaa_forecast_import")
        else:
            r = con.execute("select * from noaa_forecast limit 1")
            table_cols = [desc[0] for desc in r.description]
            for col in table_cols:
                if col not in df.columns:
                    df[col] = None
            con.register('noaa_forecast_import', df)
            logger.info("Deleting rows from noaa_forecast")
            con.execute("delete from noaa_forecast where date_time >= (select min(date_time) from noaa_forecast_import)")
            logger.info("Inserting %s rows into noaa_forecast table" % len(df))
            col_str = ', '.join(table_cols)
            con.execute(f"insert into noaa_forecast select {col_str} from noaa_forecast_import")
        con.execute("DROP VIEW noaa_forecast_import")

        # observations
        logger.debug(f"Observations: {len(observations)}")
        table_exists = False
        df = pd.DataFrame(observations)
        for col in df.columns:
            if re.match(r'.*(latit|longi|elev|chance|temp$|dewp|relh|wind|gust|visibility|altimeter)', col):
                df[col] = pd.to_numeric(df[col], errors='coerce')
        df['creation_date'] = pd.to_datetime(df['creation_date']).dt.tz_localize(None)
        df['created_at'] = datetime.utcnow()

        try:
            r = con.execute("select max(creation_date) from noaa_observation")
            row = r.fetchone()
            table_exists = True
            if row[0] is not None:
                latest_timestamp = row[0]
                logger.debug(f"Last timestamp: {latest_timestamp}")
        except Exception as e:
            e_str = str(e).replace('\n',' ')
            logger.error(f"Error getting last timestamp: {e_str}")

        if not table_exists:
            con.register('noaa_observation_import', df)
            logger.info("Creating new noaa_observation table with %s rows" % len(df))
            con.execute("CREATE TABLE noaa_observation AS SELECT * FROM noaa_observation_import")
        else:
            r = con.execute("select * from noaa_observation limit 1")
            table_cols = [desc[0] for desc in r.description]
            for col in table_cols:
                if col not in df.columns:
                    df[col] = None
            con.register('noaa_observation_import', df)
            logger.info("Inserting %s rows into noaa_observation table" % len(df))
            table_cols = ', '.join(table_cols)
            con.execute(f"insert into noaa_observation select {table_cols} from noaa_observation_import")
        con.execute("DROP VIEW noaa_observation_import")

        con.close()
