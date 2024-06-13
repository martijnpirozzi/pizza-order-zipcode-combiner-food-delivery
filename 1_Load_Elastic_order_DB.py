"""
Final Assignment Data Analysis and Programming for Operations Management
Code developed by:      Martijn de Jonge - Pirozzi
Student number:         S4147286
Start date:             15-10-2020

Goal of the code: create db (index) in Elasticsearch to load orders.csv


"""
# clear everything, everytime we run the script
%reset -f

# loading modules that we use
from datetime import datetime

from elasticsearch_dsl import Search
from elasticsearch import Elasticsearch


# define the query and connect to the ES database(= index)
es = Elasticsearch([{'host':'127.0.0.1', 'port': 9200}])

settings = {
    'settings': {
        "number_of_shards" : 2
    },
        'mappings': {
        'properties': {
            'address': {
                'type': 'text',
                'fields': {
                    'keyword': {
                        'type': 'keyword',
                        'ignore_above': 256
                    }
                }
            },
            'dateTime': {
                'type': 'date',
                'format': 'yyyy-MM-dd HH:mm:ss',
             },
            'dist': {
                'type': 'float',
                },
        'postcodeFrom': {
            'type': 'text',
            'fields': {
                'keyword': {
                    'type': 'keyword',
                    'ignore_above': 6
                }
            }
        },
        'postcodeTo': {
            'type': 'text',
            'fields': {
                'keyword': {
                    'type': 'keyword',
                    'ignore_above': 6
                }
            }
        },
        'restaurant': {
            'type': 'text',
            'fields': {
                'keyword': {
                    'type': 'keyword',
                    'ignore_above': 256
                }
            }
        },
        'tm': {
            'type': 'float',
        }
        }
    }
}
# indices.delete used to delete a bad index that I made
#es.indices.delete(index='dapom_orders', ignore=[400, 404])

# creating new index with the name dapom_orders and settings as db layout
# es.indices.create(index='dapom_orders', body=settings, ignore=400)

# show that the code was executed successfully
print("index created")
