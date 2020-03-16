import csv
import cx_Oracle
import boto3
import os
import click
import datetime
from dateutil.relativedelta import relativedelta


#define db connection object
def setup_connection(db_host,db_user,db_pass,db_service):
    global db_con
    db_con = cx_Oracle.connect('{0}/{1}@{2}/{3}'.format(db_user,db_pass,db_host,db_service))

    return

#define function that generates dictionary based on multiple options input 
def generate_options_dictionary(options_input):
    options_dict = {}
    if options_input:
        for option in options_input.split(','):
            key_pair = option.split('=')
            if len(key_pair) == int(2): 
                options_dict[key_pair[0]] = key_pair[1]
            elif len(key_pair) == int(1):
                options_dict[key_pair[0]] = True

    return options_dict

#define function to copy local file to s3
def move_file_to_s3(local_path,input_file,s3_options_dict):
    #define S3 options from dictionary
    s3_bucket = s3_options_dict.get('s3_bucket',False)
    s3_path = s3_options_dict.get('s3_path',False)

    if s3_bucket:
        s3_target = "{0}/{1}".format(s3_path,input_file) if s3_path else "{0}".format(input_file)
        print("Moving file: {0}/{1} to s3 bucket: {2}/{3}...".format(local_path,input_file,s3_bucket,s3_target))
        s3.upload_file('{0}/{1}'.format(local_path,input_file),s3_bucket,s3_target)
        print("Move completed")
        print("Deleting local file: {0}/{1}...".format(local_path,input_file))
        os.remove('{0}/{1}'.format(local_path,input_file))
        print("Deletion completed")

    return

#generate ddl file
def generate_ddl_file(db_schema,db_table,local_path,base_filename,advanced_options_dict,s3_options_dict):
    #define advanced options from dictionary
    generate_ddl = advanced_options_dict.get('generate_ddl',False)

    if generate_ddl:
        print("Generating DDL file")
        ddl_filename = "{0}.sql".format(base_filename)
        
        sql_ddl = "select dbms_metadata.get_ddl('TABLE','{1}') as table_ddl from all_tables where owner = '{0}' and table_name='{1}'".format(db_schema,db_table)
        ddl_cursor = db_con.cursor()
        ddl_cursor.execute(sql_ddl)

        ddl_file = open("{0}/{1}".format(local_path,ddl_filename),"w")
        for row in ddl_cursor.fetchone():
            ddl_file.write(str(row))
        
        ddl_file.close()
        ddl_cursor.close()
        print("File {0}/{1} created".format(local_path,ddl_filename))

        #move file to s3
        move_file_to_s3(local_path,ddl_filename,s3_options_dict)

    else:
        ddl_filename =  ''

    return ddl_filename

#generate export file
def generate_export_file(db_schema,db_table,local_path,base_filename,advanced_options_dict,s3_options_dict,date_options_dict):
    #define advanced options from dictionary
    exclude_header = advanced_options_dict.get('exclude_header',False)
    exclude_data = advanced_options_dict.get('exclude_data',False)

    #define date options from dictionary
    date_column = date_options_dict.get('date_column',False)
    start_date = date_options_dict.get('start_date',False)
    end_date = date_options_dict.get('end_date',False)
    split_period = date_options_dict.get('split_period',False)
    convert_to_julian = date_options_dict.get('convert_to_julian',False)

    if exclude_data == False or exclude_header == False:
        if date_column or start_date or end_date or split_period or convert_to_julian:
            if date_column and start_date and end_date and split_period:
                print("Starting export process using date options...")
                export_filename = ''

                #set initial dates
                start_date = datetime.datetime.strptime(start_date,'%Y-%m-%d')
                end_date = datetime.datetime.strptime(end_date,'%Y-%m-%d')

                #set period_delta based on input
                if split_period == 'days':
                    period_delta = relativedelta(days=+1)
                elif split_period == 'months':
                    period_delta = relativedelta(months=+1)
                elif split_period == 'years':
                    period_delta = relativedelta(years=+1)
                else:
                    period_delta = ''
        
                #loop through the dates based on period_delta
                while start_date <= end_date:
                    process_date_start = datetime.datetime.strftime(start_date,'%Y%m%d')
                    process_date_end = datetime.datetime.strftime(start_date+period_delta,'%Y%m%d')

                    export_filename = "{0}_{1}.csv".format(base_filename,process_date_start)

                    #set where clause for when exporting only metadata
                    if exclude_data:
                        where_clause = '1=0'
                    else:
                        where_clause = '1=1'

                    #set SQL cursor
                    if convert_to_julian:
                        sql_data = "select * from {0}.{1} where {2} and {3} >= to_char(to_date('{4}','YYYYMMDD'),'J') and {3} < to_char(to_date('{5}','YYYYMMDD'),'J')".format(db_schema,db_table,where_clause,date_column,process_date_start,process_date_end)
                    else:
                        sql_data = "select * from {0}.{1} where {2} and {3} >= to_date('{4}','YYYYMMDD') and {3} < to_date('{5}','YYYYMMDD')".format(db_schema,db_table,where_clause,date_column,process_date_start,process_date_end)

                    data_cursor = db_con.cursor()
                    data_cursor.execute(sql_data)

                    #initalize file
                    export_file = open("{0}/{1}".format(local_path,export_filename),"w")
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
                    print("File {0}/{1} created".format(local_path,export_filename))

                    #move file to s3
                    move_file_to_s3(local_path,export_filename,s3_options_dict)

                    start_date = start_date + period_delta
            else:
                print("Date options require input for: date_column, start_date, end_date, split_period")
                export_filename = ''
        else:
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
            export_file = open("{0}/{1}".format(local_path,export_filename),"w")
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
            print("File {0}/{1} created".format(local_path,export_filename))

            #move file to s3
            move_file_to_s3(local_path,export_filename,s3_options_dict)

    else:
        export_filename = ''

    return export_filename

#define function to call all required file genereation functions
def generate_files(db_schema,db_table,local_path,s3_options,advanced_options,date_options):
    #define filename & path
    base_filename = "{0}_{1}".format(db_schema,db_table)
    local_path = local_path if local_path else '.'

    #define advanced options dictionary
    advanced_options_dict = generate_options_dictionary(advanced_options)

    #define S3 options dictionary
    s3_options_dict = generate_options_dictionary(s3_options)

    #define date options dictionary
    date_options_dict = generate_options_dictionary(date_options)

    #call function to create ddl export
    generate_ddl_file(db_schema,db_table,local_path,base_filename,advanced_options_dict,s3_options_dict)
    
    #call function to create data/header export
    generate_export_file(db_schema,db_table,local_path,base_filename,advanced_options_dict,s3_options_dict,date_options_dict) 

    #Close global connection
    db_con.close()

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
@click.option('--s3_options', default=None,
    help="S3 related options (separated by comma):\n bucket_name=<s3_bucket>\n s3_path=<path>")
@click.option('--advanced_options', default=None,
    help="Advanced options (separated by comma):\n generate_ddl\n exclude_data\n exclude_header")
@click.option('--date_options', default=None,
    help="Date options (separated by comma):\n date_column=<column_name>\n start_date=<YYYY-MM-DD>\n end_date=<YYYY-MM-DD>\n split_period=<days/months/years>\n convert_to_julian")
def export(db_schema,db_table,local_path,s3_options,advanced_options,date_options):
    #Generate files
    generate_files(db_schema,db_table,local_path,s3_options,advanced_options,date_options)

    return

#main script
if __name__ == '__main__':
    cli()
