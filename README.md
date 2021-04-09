# redaql
redash sql cli client(use [redash-py](https://github.com/denzow/redash-py).
I like `psql`(PostgreSQL CLI). so redaql resemble psql in some respects.

## How To Use

redaql need some arguments or environment variables.
redaql prioritizes arguments over environment variables.

|argument|env|mean|required|
|--|--|--|--|
|-k/--api-key|REDASH_API_KEY|API KEY(user api key)|True|
|-s/--server-host|REDASH_SERVICE_URL|Redash server hostname. ex) https://your.redash.server.host/|True|
|-p/--proxy|REDASH_HTTP_PROXY|if your redash server restricted by Proxy, set url format. ex)http://user:pass@your.proxy.server:proxy-port|False|
|-d/--data-source-name||initial connect datasource name.|False|

if you want to use redaql with direnv, rename `.envrc.sample` to `.envrc` and set attributes.

### special commands

redaql has management commands.

```
\c: SELECT DATASOURCE.
\q: exit.
\d: describe table.
\x: query result toggle pivot.
\l: Load Query from Redash.
\?: HELP SP COMMANDS.
```

### execute query

see below

#### start
```
$ redaql

   ___         __          __
  / _ \___ ___/ /__ ____ _/ /
 / , _/ -_) _  / _ `/ _ `/ / 
/_/|_|\__/\_,_/\_,_/\_, /_/  
                     /_/     
  - redash query cli tool -

SUCCESS CONNECT
- server version 8.0.0+b32245
- client version 0.0.5.1

(No DataSource)=#
```

#### connect datasource

use `\c data_source_name`. if not provide data_source_name, show all available data sources. 

```
(No DataSource)=# \c metadata
metadata=#
```

#### describe table

use `\d table_name`. if not provide table_name, show all table names. if provide table_name with wildcard(\*), show describe matched tables.

```
metadata=# \d
access_permissions
alembic_version
:
queries
query_results
query_snippets
users
visualizations
widgets
metadata=# \d queries
## queries
- schedule
- updated_at
- api_key
- name
- id
- version
- is_draft
- query
- is_archived
- tags
- last_modified_by_id
- org_id
- options
- query_hash
- description
- latest_query_data_id
- search_vector
- data_source_id
- schedule_failures
- created_at
- user_id
metadata=# \d query_*
## query_results
- id
- data
- org_id
- query_hash
- data_source_id
- runtime
- query
- retrieved_at
## query_snippets
- updated_at
- id
- description
- created_at
- user_id
- trigger
- snippet
- org_id

```

#### execute query

enter your SQL and semicolon.

```bash
metadata=# select count(*) from queries;
+-------+
| count |
+-------+
|  2988 |
+-------+
1 row returned.
```

`\x` pivot result.



```
metadata=# \x
set pivot format

metadata=# select id, user_id from queries limit 3;
-------
id     : 543
user_id: 40
-------
id     : 717
user_id: 40
-------
id     : 515
user_id: 38

3 rows returned.
```

### quit

`ctrl + D` or `\q` quit redaql.

```
metadata=# \q
Bye.
```
