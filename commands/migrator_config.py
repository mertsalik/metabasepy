from dataclasses import dataclass

@dataclass
class MetabaseConnectionCredentials:
    username: str
    password: str
    base_url: str
    db_id: str

@dataclass
class MigratorConfig:
    source_credentials: MetabaseConnectionCredentials
    destination_credentials: MetabaseConnectionCredentials
    source_collection_id: int
