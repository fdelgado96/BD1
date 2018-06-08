# Base de Datos I TPE
## Setup
first check if you have postgres installed in your machine by running the following command making sure postgres version is 9.5+
```
psql --version
```

If you dont have postgres intalled do so by running the following commands
```
sudo apt-get update
sudo apt-get install postgresql postgresql-contrib
```

create the database
```
sudo -u postgres createdb bd_tpe
```

Create the **recorrido_final** table, this assumes your current directory is the project root
```
sudo -u postgres psql -d bd_tpe -a -f src/scripts/create_table.sql
 ```


## Authors


* **Francisco Delgado**
* **Martina Scomazzon**
* **Federico Mammone**
