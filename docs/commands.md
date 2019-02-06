

# Commands

## exporter: Download Cards (sql queries) into local machine 

Simply create a configuration file for example: `query_export_config.json`

Add your credentials of metabase account into that file:

    [
        {
          "name": "Metabase Account",
          "username": "john.doe@domain.com",
          "password": "******",
          "base_url": "http://localhost:3000"
        },
        {
          "name": "Another Metabase Account",
          "username": "john.doe@domain.com",
          "password": "-------",
          "base_url": "http://your-remote-metabase-url.com"
        }
    ] 

```bash
exporter -c /your/config/file/path.json -d /export_directory
```

or 

```bash
cd metabasepy

python commands/exporter.py -c /your/config/file/path.json -d /export_directory
```

Your sql queries will be saved into `/export_directory`

## flusher: Delete all cards (sql queries) defined on metabase server

Create a configuration file for example: `flusher_config.json`

Add your credentials of metabase account into that file:

    {
      "name": "Metabase A",
      "username": "john.doe@email.com",
      "password": "******",
      "base_url": "http://localhost:3000"
    }

Than run:

```bash
flusher -c /your/config/file/path.json 
```

or

```
cd metabasepy

python metabasepy/flusher.py -c /your/config/file/path.json
```


## migrator: Copy cards (sql queries) from one server to another

Create a configuration file for example: `migrator_config.json`

Add source & destination server credentials with specified database mapping into this configuration:

    {
      "source": {
        "name": "Metabase Server A",
        "username": "john.doe@email.com",
        "password": "*******",
        "base_url": "https://localhost:3001"
      },
      "destination": {
        "name": "Metabase Server B",
        "username": "john.doe@email.com",
        "password": "************",
        "base_url": "http://localhost:4000"
      },
      "mappings": {
        "databases": [
          {
            "source": "books_db_a",
            "destination": "books_db_b"
          },
          {
            "source": "sales_db_a",
            "destination": "sales_db_b"
          }
        ]
      }
    }

than simpy run:

```bash
migrator -c /your/config/file/path.json 
```

or 

```bash
cd metabasepy

python commands/migrator.py -c /your/config/file/path.json
```

program will be trying to create every card from source to destination metabase server.
