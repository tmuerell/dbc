select datname, pg_size_pretty(pg_database_size(datname))
            from pg_database
            order by pg_database_size(datname) desc