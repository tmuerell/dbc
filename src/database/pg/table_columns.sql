select ordinal_position as position,
        column_name,
        data_type,
        case when character_maximum_length is not null
             then character_maximum_length
             else numeric_precision end as max_length,
        is_nullable,
        column_default as default_value
 from information_schema.columns
 where table_name = $1
 order by ordinal_position