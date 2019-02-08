# Guide

## Client

first instantiate a client object

```python
from metabasepy import Client

cli = Client(username="XXX", password="****", base_url="https://your-remote-metabase-url.com")

```

than you can simply authenticate with

```python
cli.authenticate()
```
### Add Card to server

Save new card with custom sql query:

```python
cli.cards.post(database_id=1,
    name="Card Name (eg: Available Stocks)",
    query="select * from your_db.table_name;")

```

### List available Databases

```python
all_dbs = cli.databases.get()
print(all_dbs.__dict__)
```

### Create new Collection

```python
cli.collections.post(name="Finance Reports", color="#ff0000")
```

### List all collections

```python
cli.collections.get()
```

You can also query collection by its id:

```python
cli.collections.get(collection_id=1)
```

### Query SQL Data from Card

```python
from metabasepy import Client, MetabaseTableParser

cli = Client(username="XXX", password="****", base_url="https://your-remote-metabase-url.com")
query_response = cli.cards.query(card_id="1")

data_table = MetabaseTableParser.get_table(metabase_response=query_response)
print(data_table.__dict__)
```

Now you have table of query results

    {
        'status': 'completed',
        'native_query': 'select \n u.first_name as "First Name",
                                \n u.last_name as "Last Name", 
                                \n t.amount as "Amount",
                                \n t.description as "Description",
                                \n t.created_date as "Transaction Date"
                         from users_user u
                         inner join transactions_transaction t
                         on
                         t.user_id = u.id
                         where t.channel_id = 4;
                        ',
        'columns': [
                'First Name',
                'Last Name',
                'Amount',
                'Description',
                'Transaction Date'
        ],
        'rows': [...],
        'database': 4
    }

Than you can loop through rows & columns
    
```python
for heading in data_table.columns:
    print(heading)
```
