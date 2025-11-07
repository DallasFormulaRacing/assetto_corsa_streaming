from azure.eventhub import EventHubProducerClient, EventData, EventDataBatch
import json
import pandas as pd
import time
from datetime import timedelta
from ldparser import *
from tqdm import tqdm
from dotenv import load_dotenv
import os

load_dotenv()

class mock_streamer:
    def __init__(self, filename):
        eventhub_name = os.getenv("EVENTHUB_NAME")
        eventhub_connection_string = os.getenv("EVENTHUB_CONNECTION_STRING")

        # configure event hub client
        self.client = EventHubProducerClient.from_connection_string(eventhub_name=eventhub_name, 
                                                                    conn_str=eventhub_connection_string, 
                                                                    on_error=self.on_error, 
                                                                    on_success=self.on_success)
        self.filename = filename

        # send data
        try:
            self.send(self.start())
            self.assetto_corsa_data_transform()
            self.send(self.stop())
        except KeyboardInterrupt as e:
            self.send(self.error())

    def send(self, event):
        with self.client:
            self.client.send_event(event)

    def send_batch(self, batch):
        with self.client:
            self.client.send_batch(batch)

    def on_success(self, events, partition_id):
        # print("Success")
        pass

    def on_error(self, events, partition_id, error):
        print(error)

    def start(self):
        self.session_id = "test-3"
        start_obj = {
                        "event": {
                            "event_type": "session",
                            "data": {
                                "id": self.session_id,
                                "status": "Open", # CASE SENSITIVE
                                "start": time.time()
                            }
                        }
                    }
        return EventData(json.dumps(start_obj))

    def stop(self):
        close_obj = {
                        "event": {
                            "event_type": "session",
                            "data": {
                                "id": self.session_id,
                                "status": "Closed", # CASE SENSITIVE
                                "stop": time.time()
                            }
                        }
                    }
        return EventData(json.dumps(close_obj))

    def error(self):
        error_obj = {
                        "event": {
                            "event_type": "session",
                            "data": {
                                "id": self.session_id,
                                "status": "Error", # CASE SENSITIVE
                                "stop": time.time()
                            }
                        }
                    }
        return EventData(json.dumps(error_obj))

    def assetto_corsa_data_transform(self):
        ld = ldData.fromfile(self.filename)
        df = pd.DataFrame(data={c: ld[c].data for c in ld})

        for col in df.columns:
            if df[col].nunique() == 1:
                df.drop(columns=[col], inplace=True)

        # updates column names to sensor id for inserting into db
        sensors = pd.read_csv('railway_public_sensors.csv')
        sensors.drop(columns=['car', 'group', 'type'], inplace=True)
        sensor_dict = {}
        for idx, row in sensors.iterrows():
            sensor_dict.update({row['name']: row['sensor_id']})

        df.rename(columns=sensor_dict, inplace=True)

        event_list = self.create_events(df)
        batch_list = self.group_events(event_list)

        print('sending...')
        
        # send all event batches
        for batch in tqdm(batch_list):
            self.send_batch(batch)

        print("Complete")

    # creates list of events from dataframe
    # each row is an event with each cell being the items in the data array
    def create_events(self, df: pd.DataFrame) -> list[EventData]:
        event_list = []
        print('Parsing data to events...')
        start = time.time()

        # create list of EventData objects with the metrics
        for idx, row in tqdm(df.iterrows(), total=df.shape[0]):
            data = {
                "event": {
                    "event_type": "metrics",
                    "data": [
                        # {
                        #     "time": "datetime",
                        #     "sensor_id": "int",
                        #     "data": "float",
                        # }
                    ]
                }
            }

            tmp = row.to_dict()
            for k,v in tmp.items(): # adds each data from each sensor to the metric
                data["event"]["data"].append({'time': start + idx, 'sensor_id': k, 'data': v})
            
            event_list.append(EventData(json.dumps(data)))

    # Groups events for batching
    def group_events(self, event_list: EventData) -> list[EventDataBatch]: 
        batch_list = []
        batch_size = 50

        # Creates batches of events to send together, data is sent faster and with less requests
        for i in range(0, len(event_list), batch_size):
            batch = EventDataBatch()
            for j in range(0, batch_size):
                # break once end of list is reached
                if(i+j >= len(event_list)): 
                    break

                batch.add(event_list[i+j])
            
            batch_list.append(batch)
        
        return batch_list

if __name__ == '__main__':
    filename = input("Enter file path: ")
    try:
        mock_streamer(filename)
    except Exception as e:
        print(e.msg())
    