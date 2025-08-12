# qdb-blaster
Blast QuestDB with test data.

* Send data to multiple tables over ILP.
* Each table has a pool of threads.
* The data is randomly generated.

## Set up your database
For ingestion, in enterprise you need to set up appropriate users and permissions

```sql
CREATE USER test_user WITH PASSWORD 'pass';
GRANT HTTP, PGWIRE TO test_user;

GRANT CREATE TABLE TO test_user;
GRANT ADD COLUMN, INSERT ON ALL TABLES TO test_user;
GRANT SELECT ON ALL TABLES TO test_user;

GRANT DROP TABLE ON ALL TABLES TO test_user;
GRANT SELECT ON ALL TABLES TO test_user;
GRANT HTTP TO test_user;
GRANT CREATE TABLE TO test_user;
GRANT ADD COLUMN, INSERT ON ALL TABLES TO test_user;

-- for ILP/HTTP
ALTER USER test_user CREATE TOKEN TYPE REST WITH TTL '3650d';

-- for ILP/TCP
CREATE SERVICE ACCOUNT test_ilp_user;
GRANT CREATE TABLE TO test_ilp_user;
GRANT ILP TO test_ilp_user;
GRANT INSERT, ADD COLUMN ON ALL TABLES TO test_ilp_user;
ALTER SERVICE ACCOUNT test_ilp_user CREATE TOKEN TYPE JWK;
```

## Configuring the blaster
* Copy a config.
* Alter the connection settings adding in any tokens and passwords as appropriate.
* Generate the config with a script if too big (see `gen_big_toml.py`)

## Running the blaster
```
cargo run --release path_to_config.toml
```
