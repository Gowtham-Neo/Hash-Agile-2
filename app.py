from elasticsearch import Elasticsearch
import pandas as pd
import numpy as np


es = Elasticsearch(
    [{'host': 'localhost', 'port': 8989, 'scheme': 'http'}],
    basic_auth=("elastic", "elastic")
)

def createCollection(collection_name):
    index_name = collection_name.lower() 
    try:
        if not es.indices.exists(index=index_name):
            es.indices.create(index=index_name)
            print(f"Collection {index_name} created successfully.")
        else:
            print(f"Collection {index_name} already exists.")
    except Exception as e:
        print(f"Error creating collection: {e}")

def indexData(collection_name, exclude_column):
    index_name = collection_name.lower()
    try:
        df = pd.read_csv("./Employee Sample Data 1.csv", encoding='ISO-8859-1')
        
        if exclude_column in df.columns:
            df = df.drop(columns=[exclude_column])
        else:
            print(f"Warning: Column '{exclude_column}' not found in the DataFrame. Skipping exclusion.")
        
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        df.dropna(inplace=True)
        data = df.to_dict(orient='records')
        
        if data:
            for record in data:
                es.index(index=index_name, document=record)
            print(f"Indexed {len(data)} records into {index_name}.")
        else:
            print("No data to index.")
    except FileNotFoundError:
        print("Error: The specified CSV file was not found. Please check the file path.")
    except pd.errors.EmptyDataError:
        print("Error: The CSV file is empty.")
    except pd.errors.ParserError:
        print("Error: There was a problem parsing the CSV file.")
    except UnicodeDecodeError as e:
        print(f"Error: Unable to decode the CSV file: {e}. Try using a different encoding.")
    except Exception as e:
        print(f"Error indexing data: {e}")

def searchByColumn(collection_name, column_name, column_value):
    index_name = collection_name.lower()
    query = {
        "query": {
            "match": {
                column_name: column_value
            }
        }
    }
    try:
        results = es.search(index=index_name, body=query)
        return {'count': results['hits']['total']['value'], 'documents': results['hits']['hits']}
    except Exception as e:
        print(f"Error searching collection {index_name}: {e}")
        return {'count': 0, 'documents': []}

def getEmpCount(collection_name):
    index_name = collection_name.lower()
    try:
        results = es.count(index=index_name)
        return results['count']
    except Exception as e:
        print(f"Error getting employee count: {e}")
        return 0

def delEmpById(collection_name, employee_id):
    index_name = collection_name.lower()
    try:
        query = {
            "query": {
                "match": {
                    "_id": employee_id
                }
            }
        }
        es.delete_by_query(index=index_name, body=query)
        print(f"Employee with ID {employee_id} deleted successfully.")
    except Exception as e:
        print(f"Error deleting employee with ID {employee_id}: {e}")

def getDepFacet(collection_name):
    index_name = collection_name.lower()
    query = {
        "size": 0,
        "aggs": {
            "department_count": {
                "terms": {
                    "field": "Department.keyword"
                }
            }
        }
    }
    try:
        results = es.search(index=index_name, body=query)
        return results['aggregations']['department_count']['buckets']
    except Exception as e:
        print(f"Error getting department facets: {e}")
        return []

# Variables for testing
v_nameCollection = "Hash_Gowtham"
v_phoneCollection = "Hash_0244"

# Uncomment to test each function
# createCollection(v_nameCollection)
# createCollection(v_phoneCollection)

# indexData(v_nameCollection, 'Department')
# indexData(v_phoneCollection, 'Gender')

# print("Employee count in Hash_Gowtham:", getEmpCount(v_nameCollection))

# delEmpById(v_nameCollection, 'E02003')

# print("Employee count in Hash_Gowtham after deletion:", getEmpCount(v_nameCollection))

# print("Search by Department (Engineering):", searchByColumn(v_phoneCollection, 'Department', 'Finance'))
# print("Search by Gender (Male):", searchByColumn(v_nameCollection, 'Gender', 'Male'))
# print("Search by Department (IT) in Hash_0244:", searchByColumn(v_phoneCollection, 'Department', 'IT'))

# print("Department facets in Hash_Gowtham:", getDepFacet(v_nameCollection))
# print("Department facets in Hash_0244:", getDepFacet(v_phoneCollection))
