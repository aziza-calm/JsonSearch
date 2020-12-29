import pandas as pd

pd.set_option('display.max_columns', None)

target = pd.read_json('target.json', lines=True)
# print(target.columns)
selected_target = target.loc[:, ['EVENT_NAME', 'CLIENT_PIN', 'EVENT_DTTM', 'PRODUCT_CD']]

states = pd.read_json('states.json', lines=True)
# print(states.columns)
selected_states = states.loc[:, ['client_pin', 'product', 'token', 'timestamp', 'kafka_partition']]
selected_states[selected_states['client_pin'] == 'ARMIMW'].to_csv('states.csv')
