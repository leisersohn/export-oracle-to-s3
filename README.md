# export-oracle-to-s3
Utility to export data from Oracle to S3

## About
This utility is intended to assist in exporting data from an Oracle DB to CSV files that will reside on S3 buckets (AWS)

## Prerequisites
The following modules need to be installed: 'boto3', 'click', 'cx_Oracle'
Please note 'cx_Oracle' module requires an installation of the Oracle client (Installation instructions: https://thehelpfuldba.com/index.php/2017/09/13/installing-the-oracle-client-12c-on-an-aws-linux-ec2-silent-mode/)

## Running
* help
`python3 export_data.py --help`

`python3 export_data.py normal_export --help`

`python3 export_data.py advanced_export --help`
