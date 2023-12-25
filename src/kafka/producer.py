
try:
    from kafka import KafkaProducer
    from faker import Faker
    import json
    import pandas as pd
    from time import sleep
except Exception as e:
    pass

producer = KafkaProducer(bootstrap_servers='localhost:9092')
_instance = Faker()
path = '/Users/grawa1/Desktop/Open_Source/Kafka/src/data/HouseData.xlsx'
message = pd.read_excel(path,engine='openpyxl').to_dict(orient='records')

# for _ in range(5):
#     _data = {
#         "first_name": _instance.first_name(),
#         "city":_instance.city(),
#         "phone_number":_instance.phone_number(),
#         "state":_instance.state(),
#         "id":str(_)
#     }
#     _payload = json.dumps(_data).encode("utf-8")
#     response=producer.send('FirstTopic',_payload)
#     print(_data)

for row in message:
    serialized_message = json.dumps(row).encode("utf-8")
    print(serialized_message)
    producer.send('FirstTopic',value=serialized_message)

    sleep(2)

producer.flush() #Clear data from kafka server