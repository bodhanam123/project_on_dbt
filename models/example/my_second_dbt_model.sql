<<<<<<< HEAD
--test12
=======
--test
>>>>>>> 1ee7505b7d6cf07f2d1180aef15ea0dd2949ccac
-- Use the `ref` function to select from other models

select *
from {{ ref('my_first_dbt_model') }}
where id = 1
