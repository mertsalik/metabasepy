
### metabasepy

Python wrapper for metabase rest api



#### Export All Questions ( cards )


```
from metabasepy.client import Client, AuthorizationFailedException

metabase_client_config = {
	'username': 'foo',
	'password': 'bar',
	'base_url': 'http://localhost:3000'
}

cli = Client(**metabase_client_config)
try:
	cli.authenticate()
except AuthorizationFailedException as ex:
	# checkout your configurations
	raise 

all_cards = cli.cards.get()

for card_info in all_cards:
	card_name = card_info.get('name', "Question")
	sql_query = card_info['dataset_query']['native']['query']

	print(card_name)
	print(sql_query)

```


Get Card Data 

```

from metabasepy import Client, MetabaseTableParser

metabase_client_config = {
	'username': 'foo',
	'password': 'bar',
	'base_url': 'http://localhost:3000'
}
cli = Client(**metabase_client_config)

query_result = cli.cards.query(card_id=1)

data_table = MetabaseTableParser.get_table(query_result)

for col in data_table.columns:
    print col
    
    
for line in data_table.rows:
    print line    

```
