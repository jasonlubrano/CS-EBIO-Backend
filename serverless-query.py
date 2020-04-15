#!/usr/bin/env python

# serverless database query - postgresql example

# Copyright 2016 Amazon.com, Inc. or its affiliates.
# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#    http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file.
# This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific language governing permissions
# and limitations under the License.

import psycopg2
import logging
import traceback
import json
from os import environ

endpoint=environ.get('ENDPOINT')
port=environ.get('PORT')
dbuser=environ.get('DBUSER')
password=environ.get('DBPASSWORD')
database=environ.get('DATABASE')

query="select TreeTagId, genetics,  species, finalCultivar, synonymText,   isConfirmed,   use, country,   genotypes,   property,  location,   CAST(TRUNC(CAST(lattitude as numeric),4) as float8),   CAST(TRUNC(CAST(longitude as numeric), 4) as float8), height,   dripline,   diameter,   fireBlight,  fruitHanging FROM public.applesprimary"

logger=logging.getLogger()
logger.setLevel(logging.INFO)


def make_connection():
    conn_str="host={0} dbname={1} user={2} password={3} port={4}".format(
        endpoint,database,dbuser,password,port)
    conn = psycopg2.connect(conn_str)
    conn.autocommit=True
    return conn 


def log_err(errmsg):
    logger.error(errmsg)
    return {"body": errmsg , "headers": {}, "statusCode": 400,
        "isBase64Encoded":"false"}

logger.info("Cold start complete.") 

def handler(event,context):

    try:
        cnx = make_connection()
        cursor=cnx.cursor()

        try:
            cursor.execute(query)
        except:
            return log_err ("ERROR: Cannot execute cursor.\n{}".format(
                traceback.format_exc()) )

        try:
            results_list=[]
            dict_keys=["tree_tag_id","genetics","species","finalCultivar","synonym","isConfirmed","use","country","genotypes","propertyOwner","treeSiteLocation", "treeLatitude","treeLongitude","treeHeight","treeDripLine","trunkDiameter","fireBlight","fruitHanging"]
            for result in cursor: 
                temp ={}
                for i in range(0,len(result)):
                    temp.update({dict_keys[i]:str(result[i])})
                results_list.append(temp)
            print(results_list)
            cursor.close()

        except:
            return log_err ("ERROR: Cannot retrieve query data.\n{}".format(
                traceback.format_exc()))


        return {"body": json.dumps(results_list), "headers": {}, "statusCode": 200,
        "isBase64Encoded":"false"}

    
    except:
        return log_err("ERROR: Cannot connect to database from handler.\n{}".format(
            traceback.format_exc()))


    finally:
        try:
            cnx.close()
        except:
            pass


if __name__== "__main__":
    handler(None,None)
