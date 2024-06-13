"""
Final Assignment Data Analysis and Programming for Operations Management
Code developed by:      Martijn de Jonge - Pirozzi
Student number:         S4147286
Start date:             15-10-2020

Goal of the code: create db (index) in Elasticsearch to load postcodes.csv
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
        "number_of_shards" : 4
    },
        'mappings': {
        'properties': {
            'src': {
            'type': 'text',
                'fields': {
                    'keyword': {
                        'type': 'keyword',
                        'ignore_above': 6
                    }
                }
            },
            'dest': {
            'type': 'text',
                'fields': {
                    'keyword': {
                        'type': 'keyword',
                        'ignore_above': 6
                    }
                }
            },
            'meters': {
            'type': 'text',
                'fields': {
                    'keyword': {
                        'type': 'keyword',
                        'ignore_above': 16
                    }
                }
            },
            'seconds': {
            'type': 'text',
                'fields': {
                    'keyword': {
                        'type': 'keyword',
                        'ignore_above': 16
                    }
                }
            }
        }
    }
}
# indices.delete used to delete a bad index that I made
# es.indices.delete(index='post_distances', ignore=[400, 404])

# creating new index with the name post_distances  and settings as db layout
# es.indices.create(index='post_distances', body=settings, ignore=400)

# show that the code was executed successfully
print("index post_distances created")
