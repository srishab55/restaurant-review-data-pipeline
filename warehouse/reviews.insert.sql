COPY reviews_data
FROM 's3://reviews/transformed/reviews_data/'
IAM_ROLE 'arn:aws:iam::random-dole:role/your-redshift-role'
FORMAT AS PARQUET;