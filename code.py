import pandas as pd
from onboard.client import RtemClient
import os
from datetime import datetime, timezone, timedelta
import pytz
from onboard.client.models import TimeseriesQuery, PointData
from onboard.client.dataframes import points_df_from_streaming_timeseries
from onboard.client.models import PointSelector
 
client = RtemClient(api_key=os.environ.get('RTEM_KEY'))
print(client.whoami())

#as list
equip = client.get_equipment_types()

#as pandas dataframe
equip_type = pd.json_normalize(client.get_equipment_types())
#get tag_name
equip_type.tag_name

#get point types
point_type = pd.DataFrame(client.get_all_point_types())  
#get tag_name
point_type.tag_name

#get Measurement types
measurement_types = pd.DataFrame(client.get_all_measurements())

#get all buildings
buildings = pd.json_normalize(client.get_all_buildings())

#attributes
buildings[['sq_ft']]
buildings[['info.customerType']]
buildings[['info.geoCity']]
#number of equip and points
buildings[['equip_count']]
buildings[['point_count']]

#not many with this
buildings[['info.yearBuilt']]
buildings[['info.floors']]

#get building 140 
all_equipment = pd.DataFrame(client.get_building_equipment(140))
#get points for equip[0] "ahu1-gallery"
ahu1_points = all_equipment.loc[0]["points"]
ahu1_points_df = pd.DataFrame(ahu1_points)

#find points for building
query = PointSelector()
query.buildings = ['140']
#query.point_types = ['Zone Temperature', 'Zone Temperature Setpoint']
query.point_types = ['Outside Air Temperature']
selection = client.select_points(query)

# Get Metadata for the sensors you would like to query
sensor_metadata = client.get_points_by_ids(selection['points'])
sensor_metadata_df = pd.DataFrame(sensor_metadata)
sensor_metadata_df[['id', 'building_id', 'first_updated', 'last_updated', 'type', 'value', 'units']]

#get time series for points
# Enter Start & End Time Stamps in UTC
# Example "2018-06-03T12:00:00Z"

# get data from the past week
#start = datetime.now(pytz.utc) - timedelta(days=7)
#end = datetime.now(pytz.utc)
start = datetime(2017,1,20,0,0,0).replace(tzinfo=tz)
end = datetime(2018,1,20,0,0,0).replace(tzinfo=tz)
print(f"from {start} to {end}")

timeseries_query = TimeseriesQuery(point_ids = selection['points'], start = start, end = end)
query_results = client.stream_point_timeseries(timeseries_query)
data = points_df_from_streaming_timeseries(query_results)
data["timestamp"] = pd.to_datetime(data['timestamp'], format='%Y-%m-%dT%H:%M:%S.%f')

#data.resample('24H', on="timestamp").mean() #24hr
#data.resample('2T', on="timestamp").mean()  #2min

#save to CSV
data.to_csv("building_140_time_series.csv", index=False)

import matplotlib.pyplot as plt
import seaborn as sns

plt.style.use('ggplot')
# This for the figure size
plt.rcParams["figure.figsize"] = (20,9)
# These are the sensors, one per column (first one is the timestamp)
cols = data.columns[1:]

for col in cols:
    sns.lineplot(data=data, x="timestamp", y=col)