select schemaname
     , sequencename
     , start_value
     , min_value
     , max_value
     , increment_by
     , cycle
     , cache_size
 from pg_sequences
 where sequencename = $1