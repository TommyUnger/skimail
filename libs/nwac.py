from datetime import datetime, timedelta
import duckdb
import pandas as pd
import requests
import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


NWAC_URL = "https://api.snowobs.com/wx/v1/station/data/timeseries/?token=71ad26d7aaf410e39efe91bd414d32e1db5d&stid=%(stations)s&source=nwac&start_date=%(start_date)s&end_date=%(end_date)s&format=json"
STATION_IDS = list(range(1, 41))
# remove 15 as it is not active
STATION_IDS.remove(15)
DB = 'weather.db'

class NWAC:
    def __init__(self):
        pass

    def get_data(self, stations, start_date, end_date):
        url = NWAC_URL % {'stations': ",".join([str(x) for x in stations]), 'start_date': start_date, 'end_date': end_date}
        logger.debug("NWAC URL: %s" % url)
        try:
            r = requests.get(url)
        except Exception as e:
            logger.error("Error getting NWAC data: %s" % e)
            return None
        data = r.json()
        cols = {}
        for c, u in data['UNITS'].items():
            cols[c] = {"unit": u}
        for v in data['VARIABLES']:
            cols[v['variable']]["name"] = v['long_name']
        rows = []
        for s in data['STATION']:
            station_row = {}
            for k, v in s.items():
                if k != 'observations':
                    if k == "meta":
                        for kk, vv in v.items():
                            station_row[kk] = vv
                    elif k == "station_note":
                        if len(v) > 0:
                            station_row["station_note_date_updated"] = v[0]["date_updated"]
                            station_row["station_note_note"] = v[0]["note"]
                            station_row["station_note_status"] = v[0]["status"]
                    else:
                        station_row[k] = v

            obs = s['observations']
            if len(obs) == 0:
                logger.debug("No observations for station %s" % station_row)
                continue
            for i, ts in enumerate(obs['date_time']):
                data_row = {"date_time": ts}
                for col in cols.keys():
                    if col in v:
                        data_row[col] = obs[col][i]
                row = {**station_row, **data_row}
                rows.append(row)
        df = pd.DataFrame(rows)
        df['date_time'] = pd.to_datetime(df['date_time']).dt.tz_localize(None)
        df['created_at'] = datetime.utcnow()
        return df
    
    def examine_data(self, data):
        for k in data.keys():
            logger.debug(f"{k}, {type(data[k])}  len: {len(data[k])}")
            if type(data[k]) == list:
                for i, d in enumerate(data[k]):
                    logger.debug(f" - {i}: {d}")
                    if i > 2:
                        break
            elif type(data[k]) == dict:
                for ii, kk in enumerate(data[k].keys()):
                    logger.debug(f" - {ii}: key: {kk}, val: {data[k][kk]}")
                    if ii > 2:
                        break


    def update_data(self):
        nwac = NWAC()
        utc_now = datetime.utcnow()
        con = duckdb.connect(DB)
        # check if table exists and get latest timestamp
        latest_timestamp = None
        table_exists = False
        try:
            r = con.execute("select max(date_time) from nwac_telemetry")
            row = r.fetchone()
            table_exists = True
            if row[0] is not None:
                latest_timestamp = row[0]
                logger.debug(f"Last timestamp: {latest_timestamp}")
        except Exception as e:
            logger.error(f"Error getting last timestamp: {str(e).replace('\n',' ')}")
        # timestamp to utc datetime object
        ts_start = utc_now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=-24*14)
        ts_end = utc_now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        if latest_timestamp is not None:
            ts_start = ts_end + timedelta(hours=-4)
        t1 = ts_start.strftime('%Y%m%d%H%M')
        t2 = ts_end.strftime('%Y%m%d%H%M')
        logger.info(f"Getting data from {ts_start.strftime('%Y-%m-%d %H:%M:%S')} to {ts_end.strftime('%Y-%m-%d %H:%M:%S')}")
        data = nwac.get_data(STATION_IDS, t1, t2)

        if not table_exists:
            con.register('nwac_telemetry_import', data)
            logger.info("Creating new nwac_telemetry table with %s rows" % len(data))
            con.execute("CREATE TABLE nwac_telemetry AS SELECT * FROM nwac_telemetry_import")
        else:
            r = con.execute("select * from nwac_telemetry limit 1")
            table_cols = [desc[0] for desc in r.description]
            for col in table_cols:
                if col not in data.columns:
                    data[col] = None
            con.register('nwac_telemetry_import', data)
            logger.info("Deleting rows from nwac_telemetry table where date_time >= %s" % ts_start.strftime('%Y-%m-%d %H:%M:%S+00:00'))
            con.execute("delete from nwac_telemetry where date_time >= '%s'" % ts_start.strftime('%Y-%m-%dT%H:%M:%S+00:00'))
            logger.info("Inserting %s rows into nwac_telemetry table" % len(data))
            # grab columns for telemetry table to ensure order and presense of columns
            col_str = ", ".join(table_cols)
            con.execute("insert into nwac_telemetry select %s from nwac_telemetry_import" % col_str)
        con.execute("DROP VIEW nwac_telemetry_import")
        con.close()

