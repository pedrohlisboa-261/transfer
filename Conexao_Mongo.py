# -*- coding: utf-8 -*-
"""
Created on Wed Dec 16 17:41:40 2020

@author: u10322
"""
from pymongo import MongoClient



#Conexão Antiga  
def connectToClaraOld(database):
    client = MongoClient('10.160.100.14', 27017,
                            username='u10322',
                            password='CXbtxMGVV8BJhJnESFgcf',
                            authSource=database,
                            authMechanism='DEFAULT',
                            readPreference='secondary')
    return client

# Conexão OpenShift Produção
def connectToClara(database):
    client = MongoClient(
        "mongodb://cargadados:3qu4t0rial2020@935b039c-f3d5-4bf4-8c40-817b3abe7a5a-0.bkvfu0nd0m8k95k94ujg.databases.appdomain.cloud:31988,935b039c-f3d5-4bf4-8c40-817b3abe7a5a-1.bkvfu0nd0m8k95k94ujg.databases.appdomain.cloud:31988,935b039c-f3d5-4bf4-8c40-817b3abe7a5a-2.bkvfu0nd0m8k95k94ujg.databases.appdomain.cloud:31988/ibmclouddb?authSource=crm&replicaSet=replset",
        ssl=True,
        ssl_ca_certs= "b179a2b4-b76a-11e9-b9ae-c61492e0d24a.ca"
    )
    return client

# Conexão OpenShift Homologação
def connectToClara_HML(database):
    client = MongoClient(
        "mongodb://admin:tn25JUFKFaatgvhPfFz5UqUA3TE9bcrwDaLjfm3w4zs3bHSsVZGWfJyhPmKwty3s7h4mjJt283jDcgN7@9948efd1-aa55-473b-9112-3127e478d23d-0.0135ec03d5bf43b196433793c98e8bd5.databases.appdomain.cloud:30762,9948efd1-aa55-473b-9112-3127e478d23d-1.0135ec03d5bf43b196433793c98e8bd5.databases.appdomain.cloud:30762,9948efd1-aa55-473b-9112-3127e478d23d-2.0135ec03d5bf43b196433793c98e8bd5.databases.appdomain.cloud:30762/ibmclouddb?authSource=admin&replicaSet=replset",
        ssl=True,
        ssl_ca_certs= r"\\Mongo_Produção_OpenShift\\09c041b8-5c74-11e9-ac1d-12d9018fe92f.ca"
    )
    return client

