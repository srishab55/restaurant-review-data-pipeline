COPY restaurant_data
FROM 's3://restaurant/data/'
IAM_ROLE 'arn:aws:iam::random_role:role/redshift-role'
FORMAT AS PARQUET;