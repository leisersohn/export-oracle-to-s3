import csv
import cx_Oracle
import boto3
import os
import click
from dateutil.relativedelta import relativedelta

#define db connection object
def setup_connection(db_host,db_user,db_pass,db_service):
    global db_con
    db_con = cx_Oracle.connect('{0}/{1}@{2}/{3}'.format(db_user,db_pass,db_host,db_service))

    return

#generate ddl file
def generate_ddl_file(db_schema,db_table,base_filename):
    print("Generating DDL file")
    ddl_filename = "{0}.sql".format(base_filename)

    sql_ddl = "select dbms_metadata.get_ddl('TABLE','{1}') as table_ddl from all_tables where owner = '{0}' and table_name='{1}'".format(db_schema,db_table)
    ddl_cursor = db_con.cursor()
    ddl_cursor.execute(sql_ddl)

    ddl_file = open(ddl_filename,"w")
    for row in ddl_cursor.fetchone():
        ddl_file.write(str(row))
    
    ddl_file.close()
    ddl_cursor.close()
    print("File {0} created".format(ddl_filename))

    return ddl_filename

#generate export file
def generate_export_file(db_schema,db_table,base_filename,remove_header):
    print("Generating export file")
    export_filename = "{0}.csv".format(base_filename)

    sql_data = "select * from {0}.{1}".format(db_schema,db_table)
    data_cursor = db_con.cursor()
    data_cursor.execute(sql_data)

    export_file = open(export_filename,"w")
    writer = csv.writer(export_file, dialect='excel')
    
    if remove_header == False:
        cols = []
        for col in data_cursor.description:
            cols.append(col[0])
        
        writer.writerow(cols)

    for row_data in data_cursor:
        writer.writerow(row_data)

    export_file.close()
    data_cursor.close()
    print("File {0} created".format(export_filename))

    return export_filename

#define s3 object
s3 = boto3.client('s3')

#define main command line group
@click.command('export_table')
@click.option('--db_host', default=None,
    help="Source DB DNS/IP address")
@click.option('--db_user', default=None,
    help="Source DB username")
@click.option('--db_pass', default=None,
    help="Source DB password")
@click.option('--db_service', default=None,
    help="Source DB service name")
@click.option('--db_schema', default=None,
    help="Source DB schema name")
@click.option('--db_table', default=None,
    help="Source DB table name")
@click.option('--local_path', default=None,
    help="Local path for temporary export")
@click.option('--generate_ddl', default=False, is_flag=True,
    help="Generate DDL file")
@click.option('--export_data', default=False, is_flag=True,
    help="Generate export file")
@click.option('--remove_header', default=False, is_flag=True,
    help="Remove header row")
def cli(db_host,db_user,db_pass,db_service,db_schema,db_table,local_path,generate_ddl,export_data,remove_header):
    """Utility to export Oracle data to S3"""
    #Setup global connection
    setup_connection(db_host,db_user,db_pass,db_service)

    #Define base file name
    base_filename = "{0}_{1}".format(db_schema,db_table)

    #Generate DDL file
    if generate_ddl:
        generate_ddl_file(db_schema,db_table,base_filename)

    #Generate export file:
    if export_data:
        generate_export_file(db_schema,db_table,base_filename,remove_header)
    
    #Close global connection
    db_con.close()
    return
#main script
if __name__ == '__main__':
    cli()