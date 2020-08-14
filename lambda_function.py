import psycopg2
import boto3
import json

def lambda_handler(event, context):
        ###check the already updated 
        import boto3
        import json
        import psycopg2
        ### if new file list is not in json
        dbname='dev'
        host_url='redshift-cluster-2.cmaq5bqoi9af.us-east-1.redshift.amazonaws.com'
        port='5439'
        user='brightbuildingllc'
        password='Brightbuildingllc123'
        #table='pp'
        #filepath='s3://brightbuilding-client-data/cornell/point_list/current_points.csv'
        aws_access_key_id='AKIA2OPDVWACZRMOL6NA'
        aws_secret_access_key='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'
        def upload_data_to_redshift(dbname,port,user,password,host_url, aws_access_key_id, aws_secret_access_key):
                conn_string = "dbname='{}' port='{}' user='{}' password='{}' host='{}'"\
                        .format(dbname,port,user,password,host_url)  
                sql="""UNLOAD ('SELECT pp.pointid,pp.timestamp,pp.value from pp JOIN pointlist ON pp.pointid=pointlist.pointid') TO 's3://brightbuilding-client-data/cornell/daily_temp_file/redshift_whole_data/' \
                        credentials 'aws_access_key_id=%s;aws_secret_access_key=%s' \
                                csv;""" \
                                        % (aws_access_key_id,aws_secret_access_key)
                try:
                        con = psycopg2.connect(conn_string)
                        print("Connection Successful!")
                        con.commit()
                except:
                        print("Unable to connect to Redshift")
                cur = con.cursor()
                try:
                        cur.execute(sql)
                        print("Copy Command executed successfully")
                        con.commit()
                except:
                        print("Failed to execute unload command")
                con.close()
        upload_data_to_redshift(dbname,port,user,password,host_url, aws_access_key_id, aws_secret_access_key)
        client = boto3.client('redshift')
        try:
                response = client.pause_cluster(ClusterIdentifier='redshift-cluster-2')
                print('already paused cluster')
                lambda_client = boto3.client('lambda')
                lambda_client.invoke(FunctionName="Redshift_Batch_Running",InvocationType='Event')
        except:
                print('cluster is not closed')