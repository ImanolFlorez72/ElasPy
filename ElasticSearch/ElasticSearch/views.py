from logging import exception
from django.template import Template, Context, Library
from django.shortcuts import render
from django.http import HttpResponse
import pandas as pd
import json
from .connect import Connect
from django.views.decorators.csrf import csrf_exempt
import csv
import base64
import mimetypes
import uuid
import os
import tempfile

def index(request):
    
    List_Index=all_index()
    #Contexto
    Context={
        "List":List_Index, 
    }
    #render
    return render(request,"index.html",Context)


def all_index():
    get=Connect()
    list=[]
    for clave in get.client.indices.get_alias():
        if not clave.startswith("."):
            list.append(clave)
        
    return sorted(list)

def get_info_index(request,index : str):
    get=Connect()
    data = get.client.indices.get(index)
    diccionario = {}
    propiedades = data[index] ["mappings"] ["properties"]
    for Prueba in propiedades :
        diccionario [Prueba] = propiedades[Prueba]["type"]
   
    Context={
        "indice" : index,
        "parameters":diccionario
    }
    #tcontex 
    return render(request,"filters.html",Context)


@csrf_exempt
def descargar_archivo(request): 
    try:
        get=Connect()
        if request.method =="POST":
            filters=request.POST
            index_name = filters['indexname']
            types = get.client.indices.get(index_name)
            diccionario = {}
            propiedades = types[index_name] ["mappings"] ["properties"]
            for Prueba in propiedades :
                diccionario [Prueba] = propiedades[Prueba]["type"]
            IsMatchAll = False;
            data = []
            field_string =[]
            body={"query":{"bool":{}}}
            for key in filters:
                if filters[key] != "" and filters[key] !="undefined" and filters[key] !=index_name:
                    IsMatchAll=False
                    if diccionario[key] == "text" or diccionario[key] == "long" or diccionario[key] == "boolean":
                        field_string.append({"match":{key:filters[key]}})
                    elif diccionario[key] == "date":
                        #if "bool" in body["query"]:
                        if "filters " not in body["query"]:
                                body["query"].update({"bool":{"filter":{"range":{}}}})
                        body["query"]["bool"]["filter"]["range"][key]={"gte" : filters[key]}
            body["query"]["bool"].update({"must":field_string})
            if IsMatchAll:
                body={
                    "query":{
                            "match_all": {}
                            }
                    }
            search_response = get.client.search(body,index = index_name,size=10000,scroll="5m")
            total_hits = search_response["hits"]["total"]["value"]
            scroll_id = search_response["_scroll_id"]
            data.extend(search_response["hits"]["hits"])
            while len(data) < total_hits:
                    scroll_response = get.client.scroll(scroll_id=scroll_id, scroll="5m")
                    scroll_id = scroll_response["_scroll_id"]
                    data.extend(scroll_response["hits"]["hits"])
            df = pd.json_normalize([item["_source"] for item in data])
            response = HttpResponse(content_type = 'text/csv')
            df.to_csv(path_or_buf=response,sep=';',encoding='utf-8')
            response['Content-Disposition'] = f"attachment; filename={index_name}_{uuid.uuid4()}.csv"
            return response
    except Exception as i:
        print("Error: "+str(i))
        List_Index=all_index()
        #Contexto
        Context={
            "List":List_Index, 
        }
        #render
        return render(request,"index.html",Context)

