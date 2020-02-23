# export-oracle-to-s3
Utility to export data from Oracle to S3

## About
This utility is intended to assist in exporting data from an Oracle DB to CSV files that will reside on S3 buckets (AWS)

## Prerequisites
The following modules need to be installed: 'boto3', 'click', 'cx_Oracle'
Please note 'cx_Oracle' module requires an installation of the Oracle client (Installation instructions: https://thehelpfuldba.com/index.php/2017/09/13/installing-the-oracle-client-12c-on-an-aws-linux-ec2-silent-mode/)

## Running 
`python3 export_data.py --help`

`python3 export_data.py normal_export --help`

`python3 export_data.py advanced_export --help`

## Examples
Table export to local copy
`python3 export_data.py --db_host example_host --db_user example_user --db_pass example_pass --db_service example_service export --db_schema example_schema --db_table example_table`

Table export moving file to s3 bucket
`python3 export_data.py --db_host example_host --db_user example_user --db_pass example_pass --db_service example_service export --db_schema example_schema --db_table example_table --s3_options s3_bucket=example_bucket,s3_path=example_bucket_path`

Table export using advanced options (excluding header & generating an extra DDL file)
`python3 export_data.py --db_host example_host --db_user example_user --db_pass example_pass --db_service example_service export --db_schema example_schema --db_table example_table --advanced_options generate_ddl,exclude_header`

Table export using period filter & split (export January-2020 into daily files)
`python3 export_data.py --db_host example_host --db_user example_user --db_pass example_pass --db_service example_service export --db_schema example_schema --db_table example_table --date_options date_column=example_column,start_date=2020-01-01,end_date=2020-01-31,split_period=days`

Table export using period filter on julian date & monthly split & extra DDL file & moving all to s3 bucket
`python3 export_data.py --db_host example_host --db_user example_user --db_pass example_pass --db_service example_service export --db_schema example_schema --db_table example_table --advanced_options generate_ddl --s3_options s3_bucket=example_bucket,s3_path=example_bucket_path --date_options date_column=example_column,start_date=2020-01-01,end_date=2020-01-31,split_period=days,convert_to_julian`