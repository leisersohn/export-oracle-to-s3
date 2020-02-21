import csv
import cx_Oracle
import boto3
import os
import click
from dateutil.relativedelta import relativedelta

#define base filename
def define_base_filename(db_schema,db_table,local_path):
    base_filename = "{0}_{1}".format(db_schema,db_table)
    if local_path:
        base_filename = "{0}/{1}".format(local_path,base_filename)

    return base_filename

#define db connection object
def setup_connection(db_host,db_user,db_pass,db_service):
    global db_con
    db_con = cx_Oracle.connect('{0}/{1}@{2}/{3}'.format(db_user,db_pass,db_host,db_service))

    return

#generate ddl file
def generate_ddl_file(db_schema,db_table,base_filename,generate_ddl):
    if generate_ddl:
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
    else:
        ddl_filename =  ''

    return ddl_filename

#generate export file
def generate_export_file(db_schema,db_table,base_filename,exclude_data,exclude_header):
    if exclude_data == False or exclude_header == False:
        print("Generating export file")
        export_filename = "{0}.csv".format(base_filename)

        #set where clause for when exporting only metadata
        if exclude_data:
            where_clause = '1=0'
        else:
            where_clause = '1=1'
        
        #set SQL cursor
        sql_data = "select * from {0}.{1} where {2}".format(db_schema,db_table,where_clause)
        data_cursor = db_con.cursor()
        data_cursor.execute(sql_data)

        #initalize file
        export_file = open(export_filename,"w")
        writer = csv.writer(export_file, dialect='excel')
        
        #add header
        if exclude_header == False:
            cols = []
            for col in data_cursor.description:
                cols.append(col[0])
            
            writer.writerow(cols)

        #add data
        for row_data in data_cursor:
            writer.writerow(row_data)

        #finish by closing file & cursor
        export_file.close()
        data_cursor.close()
        print("File {0} created".format(export_filename))
    else:
        export_filename = ''

    return export_filename

#define function to call all required file genereation functions
def generate_files(db_schema,db_table,local_path,advanced):
    base_filename = define_base_filename(db_schema,db_table,local_path)
    
    #Define advanced options
    advanced_options = advanced.split(',') if advanced else ['']
    generate_ddl = True if 'generate_ddl' in advanced_options else False
    exclude_header = True if 'exclude_header' in advanced_options else False
    exclude_data = True if 'exclude_data' in advanced_options else False

    #call function to create ddl export
    generate_ddl_file(db_schema,db_table,base_filename,generate_ddl)
    
    #call function to create data/header export
    generate_export_file(db_schema,db_table,base_filename,exclude_data,exclude_header) 

    return

#define s3 object
s3 = boto3.client('s3')

#define main command line group
@click.group()
@click.option('--db_host', default=None,
    help="Source DB DNS/IP address")
@click.option('--db_user', default=None,
    help="Source DB username")
@click.option('--db_pass', default=None,
    help="Source DB password")
@click.option('--db_service', default=None,
    help="Source DB service name")
def cli(db_host,db_user,db_pass,db_service):
    """Utility to export Oracle data to S3"""
    #Setup global connection
    if db_host:
        setup_connection(db_host,db_user,db_pass,db_service)


#define export command
@cli.command('export')
@click.option('--db_schema', default=None,
    help="Source DB schema name")
@click.option('--db_table', default=None,
    help="Source DB table name")
@click.option('--local_path', default=None,
    help="Local path for temporary export")
@click.option('--advanced', default=None,
    help="Advanced options (separated by comma):\n generate_ddl\n exclude_data\n exclude_header")
def export(db_schema,db_table,local_path,advanced):
    #Generate files
    generate_files(db_schema,db_table,local_path,advanced)

    #Close global connection
    db_con.close()
    return

#main script
if __name__ == '__main__':
    cli()