import pandas as pd
from onboard.client import RtemClient
import os
 
client = RtemClient(api_key=os.environ.get('RTEM_KEY'))
print(client.whoami())    