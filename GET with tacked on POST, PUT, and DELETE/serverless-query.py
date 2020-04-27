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

logger=logging.getLogger()
logger.setLevel(logging.INFO)


def make_connection():
    conn_str="host={0} dbname={1} user={2} password={3} port={4}".format(endpoint,database,dbuser,password,port)
    conn = psycopg2.connect(conn_str)
    conn.autocommit=True
    return conn 


def log_err(errmsg):
    logger.error(errmsg)
    return {"body": errmsg , "headers": {}, "statusCode": 400,"isBase64Encoded":"false"}

logger.info("Cold start complete.")

def handler(event,context):
    try:
        cnx = make_connection()
        cursor=cnx.cursor()
        QueryType = event['QueryType']






        if (QueryType == ""): #GET
            TTI = event['TreeTagId']
            Country = event['Country']		
            DriplineMax = event['DriplineMax']
            DriplineMin = event['DriplineMin']
            FinalCultivar = event['FinalCultivar']		
            FruitDiameterMax = event['FruitDiameterMax']
            FruitDiameterMin = event['FruitDiameterMin']
            TreeHeightMax = event['TreeHeightMax']	
            TreeHeightMin = event['TreeHeightMin']
            query="select TreeTagId, genetics,  species, finalCultivar, synonymText,   isConfirmed,   use, country,   genotypes,   property,  location,   CAST(TRUNC(CAST(latitude as numeric),4) as float8),   CAST(TRUNC(CAST(longitude as numeric), 4) as float8), height,   dripline,   diameter,   fireBlight,  fruitHanging FROM applesprimary"
            flag=0
            if(TTI!=""):
                if(flag==0):
                    query = query + " WHERE TreeTagId = " + TTI
                    flag = 1
                else:
                    query = query + " and TreeTagId = " + TTI
            if(Country!=""):
                if(flag==0):
                    query = query + " WHERE country = '" + Country + "'"
                    flag = 1
                else:
                    query = query + " and country = '" + Country + "'"
            if(DriplineMax!=""):
                if(flag==0):
                    query = query + " WHERE dripline < " + DriplineMax
                    flag = 1
                else:
                    query = query + " and dripline < " + DriplineMax
            if(DriplineMin!=""):
                if(flag==0):
                    query = query + " WHERE dripline > " + DriplineMin
                    flag = 1
                else:
                    query = query + " and dripline > " + DriplineMin
            if(FinalCultivar!=""):
                if(flag==0):
                    query = query + " WHERE finalCultivar = '" + FinalCultivar + "'"
                    flag = 1
                else:
                    query = query + " and finalCultivar = '" + FinalCultivar + "'"
            if(FruitDiameterMax!=""):
                if(flag==0):
                    query = query + " WHERE diameter < " + FruitDiameterMax
                    flag = 1
                else:
                    query = query + " and diameter < " + FruitDiameterMax
            if(FruitDiameterMin!=""):
                if(flag==0):
                    query = query + " WHERE diameter > " + FruitDiameterMin
                    flag = 1
                else:
                    query = query + " and diameter > " + FruitDiameterMax
            if(TreeHeightMax!=""):
                if(flag==0):
                    query = query + " WHERE height < " + TreeHeightMax
                    flag = 1
                else:
                    query = query + " and height < " + TreeHeightMax
            if(TreeHeightMin!=""):
                if(flag==0):
                    query = query + " WHERE height > " + TreeHeightMin
                    flag = 1
                else:
                    query = query + " and height > " + TreeHeightMin
            query = query + ";"
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
                        temp.update({dict_keys[i]:result[i]})
                    results_list.append(temp)
                print(results_list)
                cursor.close()

            except:
                return log_err ("ERROR: Cannot retrieve query data.\n{}".format(
                    traceback.format_exc()))


            return {"body": results_list, "headers": {}, "statusCode": 200,
            "isBase64Encoded":"false"}












        elif(QueryType == "xxxxxxxx"): #POST
            TreeTagId = event['TreeTagId']
            Genetics = event['Genetics']		
            Species = event['Species']
            FinalCultivar = event['FinalCultivar']
            SynonymText = event['SynonymText']		
            IsConfirmed = event['IsConfirmed']
            Use = event['Use']
            Country = event['Country']	
            Genotypes = event['Genotypes']
            Property = event['Property']
            Location = event['Location']
            Latitude = event['Latitude']
            Longitude = event['Longitude']
            Height = event['Height']
            Dripline = event['Dripline']
            Diameter = event['Diameter']
            FireBlight = event['FireBlight']
            FruitHanging = event['FruitHanging']
            if(TreeTagId!=""):
                query="Insert into applesprimary ("
                endQuery = ") values ("
                if(TreeTagId!=""):
                    query = query + "TreeTagId, "
                    endQuery = endQuery + TreeTagId + ", "
                if(Genetics!=""):
                    query = query + "Genetics, "
                    endQuery = endQuery + "'" + Genetics + "', "
                if(Species!=""):
                    query = query + "Species, "
                    endQuery = endQuery + "'" + Species + "', "
                if(FinalCultivar!=""):
                    query = query + "FinalCultivar, "
                    endQuery = endQuery + "'" + FinalCultivar + "', "
                if(SynonymText!=""):
                    query = query + "SynonymText, "
                    endQuery = endQuery + "'" + SynonymText + "', "
                if(IsConfirmed!=""):
                    query = query + "IsConfirmed, "
                    endQuery = endQuery + "'" + IsConfirmed + "', "
                if(Use!=""):
                    query = query + "Use, "
                    endQuery = endQuery + "'" + Use + "', "
                if(Country!=""):
                    query = query + "Country, "
                    endQuery = endQuery + "'" + Country + "', "
                if(Genotypes!=""):
                    query = query + "Genotypes, "
                    endQuery = endQuery + Genotypes + ", "
                if(Property!=""):
                    query = query + "Property, "
                    endQuery = endQuery + "'" + str(Property) + "', "
                if(Location!=""):
                    query = query + "Location, "
                    endQuery = endQuery + "'" + str(Location) + "', "
                if(Latitude!=""):
                    query = query + "Latitude, "
                    endQuery = endQuery + Latitude + ", "
                if(Longitude!=""):
                    query = query + "Longitude, "
                    endQuery = endQuery + Longitude + ", "
                if(Height!=""):
                    query = query + "Height, "
                    endQuery = endQuery + Height + ", "
                if(Dripline!=""):
                    query = query + "Dripline, "
                    endQuery = endQuery + Dripline + ", "
                if(Diameter!=""):
                    query = query + "Diameter, "
                    endQuery = endQuery + Diameter + ", "
                if(FireBlight!=""):
                    query = query + "FireBlight, "
                    endQuery = endQuery + FireBlight + ", "
                if(FruitHanging!=""):
                    query = query + "FruitHanging, "
                    endQuery = endQuery + "'" + FruitHanging + "', "
                query = query[:len(query)-2] + endQuery[:len(endQuery)-2] + ");"
                query2 = "select * FROM applesprimary WHERE TreeTagId = " + TreeTagId + ";"
            try:
                cursor.execute(query)
                cursor.execute(query2)
            except:
                return log_err ("ERROR: Cannot execute cursor.\n{}".format(
                    traceback.format_exc()) )
            try:
                results_list=[]
                dict_keys=["tree_tag_id","genetics","species","finalCultivar","synonym","isConfirmed","use","country","genotypes","propertyOwner","treeSiteLocation", "treeLatitude","treeLongitude","treeHeight","treeDripLine","trunkDiameter","fireBlight","fruitHanging"]
                for result in cursor: 
                    temp ={}
                    for i in range(0,len(result)):
                        temp.update({dict_keys[i]:result[i]})
                    results_list.append(temp)
                print(results_list)
                cursor.close()


            except:
                return log_err ("ERROR: Cannot retrieve query data.\n{}".format(
                    traceback.format_exc()))


            return {"body": results_list, "headers": {}, "statusCode": 200,
            "isBase64Encoded":"false"}

















        elif(QueryType == "xxxxxxxx"): #DELETE
            TreeTagId = event['TreeTagId']
            if(TreeTagId!=""):
                query="delete from applesprimary  where TreeTagId = " + TreeTagId + ";"
                query2 = "select * FROM applesprimary WHERE TreeTagId = " + TreeTagId + ";"
            try:
                cursor.execute(query)
                cursor.execute(query2)
            except:
                return log_err ("ERROR: Cannot execute cursor.\n{}".format(
                    traceback.format_exc()) )
            try:
                results_list=[]
                dict_keys=["tree_tag_id","genetics","species","finalCultivar","synonym","isConfirmed","use","country","genotypes","propertyOwner","treeSiteLocation", "treeLatitude","treeLongitude","treeHeight","treeDripLine","trunkDiameter","fireBlight","fruitHanging"]
                for result in cursor: 
                    temp ={}
                    for i in range(0,len(result)):
                        temp.update({dict_keys[i]:result[i]})
                    results_list.append(temp)
                print(results_list)
                cursor.close()


            except:
                return log_err ("ERROR: Cannot retrieve query data.\n{}".format(
                    traceback.format_exc()))


            return {"body": results_list, "headers": {}, "statusCode": 200,
            "isBase64Encoded":"false"}

        elif(QueryType == "xxxxxxxxx"): #PUT
            TreeTagId = str(event['TreeTagId'])
            Genetics = str(event['Genetics'])
            Species = str(event['Species'])
            FinalCultivar = str(event['FinalCultivar'])
            SynonymText = str(event['SynonymText'])
            IsConfirmed = str(event['IsConfirmed'])
            Use = str(event['Use'])
            Country = str(event['Country'])	
            Genotypes = str(event['Genotypes'])
            Property = str(event['Property'])
            Location = str(event['Location'])
            Latitude = str(event['Latitude'])
            Longitude = str(event['Longitude'])
            Height = str(event['Height'])
            Dripline = str(event['Dripline'])
            Diameter = str(event['Diameter'])
            FireBlight = str(event['FireBlight'])
            FruitHanging = str(event['FruitHanging'])
            if(TreeTagId!=""):
                query="Update applesprimary Set (TreeTagId, "
                endQuery = ") = (" + TreeTagId + ", "
                if(Genetics!=""):
                    query = query + "Genetics, "
                    endQuery = endQuery + "'" + Genetics + "', "
                if(Species!=""):
                    query = query + "Species, "
                    endQuery = endQuery + "'" + Species + "', "
                if(FinalCultivar!=""):
                    query = query + "FinalCultivar, "
                    endQuery = endQuery + "'" + FinalCultivar + "', "
                if(SynonymText!=""):
                    query = query + "SynonymText, "
                    endQuery = endQuery + "'" + SynonymText + "', "
                if(IsConfirmed!=""):
                    query = query + "IsConfirmed, "
                    endQuery = endQuery + "'" + IsConfirmed + "', "
                if(Use!=""):
                    query = query + "Use, "
                    endQuery = endQuery + "'" + Use + "', "
                if(Country!=""):
                    query = query + "Country, "
                    endQuery = endQuery + "'" + Country + "', "
                if(Genotypes!=""):
                    query = query + "Genotypes, "
                    endQuery = endQuery + Genotypes + ", "
                if(Property!=""):
                    query = query + "Property, "
                    endQuery = endQuery + "'" + Property + "', "
                if(Location!=""):
                    query = query + "Location, "
                    endQuery = endQuery + "'" + Location + "', "
                if(Latitude!=""):
                    query = query + "Latitude, "
                    endQuery = endQuery + Latitude + ", "
                if(Longitude!=""):
                    query = query + "Longitude, "
                    endQuery = endQuery + Longitude + ", "
                if(Height!=""):
                    query = query + "Height, "
                    endQuery = endQuery + Height + ", "
                if(Dripline!=""):
                    query = query + "Dripline, "
                    endQuery = endQuery + Dripline + ", "
                if(Diameter!=""):
                    query = query + "Diameter, "
                    endQuery = endQuery + Diameter + ", "
                if(FireBlight!=""):
                    query = query + "FireBlight, "
                    endQuery = endQuery + "'" + FireBlight + "', "
                if(FruitHanging!=""):
                    query = query + "FruitHanging, "
                    endQuery = endQuery + "'" + FruitHanging + "', "
                query = query[:len(query)-2] + endQuery[:len(endQuery)-2] + ") WHERE TreeTagId = " + TreeTagId + ";"
                query2 = "select * FROM applesprimary WHERE TreeTagId = " + TreeTagId + ";"
            try:
                cursor.execute(query)
                cursor.execute(query2)
            except:
                return log_err ("ERROR: Cannot execute cursor.\n{}".format(
                    traceback.format_exc()) )
            try:
                results_list=[]
                dict_keys=["tree_tag_id","genetics","species","finalCultivar","synonym","isConfirmed","use","country","genotypes","propertyOwner","treeSiteLocation", "treeLatitude","treeLongitude","treeHeight","treeDripLine","trunkDiameter","fireBlight","fruitHanging"]
                for result in cursor: 
                    temp ={}
                    for i in range(0,len(result)):
                        temp.update({dict_keys[i]:result[i]})
                    results_list.append(temp)
                print(results_list)
                cursor.close()


            except:
                return log_err ("ERROR: Cannot retrieve query data.\n{}".format(
                    traceback.format_exc()))


            return {"body": results_list, "headers": {}, "statusCode": 200,
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
