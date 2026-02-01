
### docker ########################################
# remove the existing containers
> docker compose ls
or
> docker-compose ps -all
> docker-compose down -rmi -v

# create directories on the host machine to mount them as docker volume
> d:
> mkdir docker_mnt
> cd docker_mnt
> mkdir pg_data
> mkdir pg_tablespace
> mkdir pg_log
> cd pg_tablespace
> mkdir mi_default

# execute docker-compose (you must be logged in to docker)
> docker-compose up -d
# show all containers
> docker container ls
or
> docker-compose ps --all

### pgadmin4 ######################################
# open url from pgadmin on the container: http://localhost:5050/
# create a server connection
name: postgresql_docker
connection:
- host: postgresql_database
- port: 5432
- username: postgres
- password: postgres

# open url from pgadmin on host machine: http://localhost:56412/
# create a server connection
name: postgresql_docker
connection:
- host: localhost
- port: 5433
- username: postgres
- password: postgres

### tablespace ####################################
# open docker cli
> docker container ls
> docker exec -it <container id> bash
$ cd /var/lib/postgresql
$ cd tablespace
$ mkdir mi_default
$ ls -l
$ chown postgres:postgres mi_default

# run query in the sql tool of pgadmin4: http://localhost:5050/
CREATE TABLESPACE mi_default OWNER postgres LOCATION '/var/lib/postgresql/tablespace/mi_default'

### database #####################################
CREATE DATABASE tr_montrg_srv
    WITH
    OWNER = postgres
    TEMPLATE = template0
    ENCODING = 'UTF8'
    LC_COLLATE = 'C'
    LC_CTYPE = 'C'
    TABLESPACE = mi_default
    CONNECTION LIMIT = -1;

GRANT ALL ON DATABASE tr_montrg_srv TO postgres;

### postgresql configuration #####################
$ cd /var/lib/postgres/data
# Adjust PostgreSQL configuration so that remote connections to the database are possible.
$ echo "host all  all    0.0.0.0/0  md5" >> /var/lib/postgresql/data/pg_hba.conf

# and add "listen_addresses" to "/var/lib/postgresql/data/postgresql.conf"
$ echo "listen_addresses='*'" >> /var/lib/postgresql/data/postgresql.conf



### restore dumped database to docker #####################################
# copy the dumped file to D:\docker_mnt\pg_data
# open the container in terminal
