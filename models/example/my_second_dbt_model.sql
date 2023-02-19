<<<<<<< HEAD
--test2
=======
<<<<<<< HEAD
--test12
=======
--test
>>>>>>> 1ee7505b7d6cf07f2d1180aef15ea0dd2949ccac
>>>>>>> 848d630b4dc318580a6be8e1782a63a4eac855e1
-- Use the `ref` function to select from other models
--
select *
from {{ ref('my_first_dbt_model') }}
where id = 1
