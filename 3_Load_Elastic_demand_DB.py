"""
Final Assignment Data Analysis and Programming for Operations Management
Code developed by:      Martijn de Jonge
Student number:         S4147286
Start date:             15-10-2020

Goal of the code:


"""
# clear everything, everytime we run the script
%reset -f

import requests
headers = { 'Content-Type': 'application/json' }
data = '{"index.blocks.read_only_allow_delete": false}'
response = requests.put('http://localhost:9200/_all/_settings', headers=headers, data=data)

# loading modules that we use
from datetime import datetime
import sys, os
import gpxpy
import gpxpy.gpx
from geopy import distance

from gurobipy import Model, GRB

import json
import smopy
import folium
import pandas as pd
import matplotlib.pyplot as plt

from elasticsearch_dsl import Search
from elasticsearch import Elasticsearch


# define the query and connect to the ES database(= index)
es = Elasticsearch([{'host':'127.0.0.1', 'port': 9200}])

search_body = {"size": 0,
    "aggs": {
        "holidays": {
            "date_range": {
                "field": "dateTime",
                "format": "dd-MM-yyyy",
                "ranges": [
                    { "from": "01-01-2018", "to": "02-01-2018" }, #New Years Day
                    { "from": "30-03-2018", "to": "31-03-2018" }, #Good Friday
                    { "from": "01-04-2018", "to": "03-04-2018" }, #Easter
                    { "from": "27-04-2018", "to": "28-04-2018" }, #Kings Day
                    { "from": "05-05-2018", "to": "06-05-2018" }, #Liberation Day
                    { "from": "10-05-2018", "to": "11-05-2018" }, #Ascension Day
                    { "from": "20-05-2018", "to": "22-05-2018" }, #Pentecost
                    { "from": "25-12-2018", "to": "27-12-2018" }  #Christmas
                ]
            },
            "aggs": { # the aggregation is going over the weekdays per hour, were results are found for
                "orders_per_hour":  {
                    "terms": { "size": 24, # specifying the amount of hours per day = results. Weird enough 0 in the beginning was not sufficient
                        "script": {
                            "lang": "painless",
                            "source": "doc['dateTime'].value.hourOfDay"
                        },
                        "order": {
                            "_key": "asc" # order it in ascending order
                        }
                    },
                    "aggs": {
                        "total_travel_time": {
                            "sum": {
                                "field": "tm" # counting the total time traveled
                            } # for every hour during de week per day
                        }
                    }
                }
            }
        }
    }
}

"""
search_body = {
    "size": 0,
        "aggs": {
            "count_per_week_day": { # count per weekday with little script
                "terms": {
                    "script": {
                        "lang": "painless",
                        "source": "doc['dateTime'].value.dayOfWeek"
                    },
                    "order": {
                        "_key": "asc" # order it in ascending order
                    }
                },
            "aggs": { # the aggregation is going over the weekdays per hour, were results are found for
                "orders_per_hour":  {
                    "terms": { "size": 24, # specifying the amount of hours per day = results. Weird enough 0 in the beginning was not sufficient
                        "script": {
                            "lang": "painless",
                            "source": "doc['dateTime'].value.hourOfDay"
                        },
                        "order": {
                            "_key": "asc" # order it in ascending order
                        }
                    },
                    "aggs": {
                        "total_travel_time": {
                            "sum": {
                                "field": "tm" # counting the total time traveled
                            } # for every hour during de week per day
                        }
                    }
                }
            }
        }
    }
}


search_body = {
    "size": 0,
    "aggs": {
        "count_per_weekend_day": { # count per weekend day with little script
            "terms": {
                "script": {
                    "lang": "painless",
                    "source": "doc['dateTime'].value.dayOfWeek == 6 || doc['dateTime'].value.dayOfWeek == 7"
                    # count days that are 6 & 7 = weekend, under key "true"
                }
            }
        }
    }
}
        "aggs": {
            "count_per_week_day": { # count per weekday with little script
                "terms": {
                    "script": {
                        "lang": "painless",
                        "source": "doc['dateTime'].value.dayOfWeek"
                    },
                    "order": {
                        "_key": "asc" # order it in ascending order
                    }
                },
            "aggs": { # the aggregation is going over the weekdays per hour, were results are found for
                "orders_per_hour":  {
                    "terms": { "size": 24, # specifying the amount of hours per day = results. Weird enough 0 in the beginning was not sufficient
                        "script": {
                            "lang": "painless",
                            "source": "doc['dateTime'].value.hourOfDay"
                        },
                        "order": {
                            "_key": "asc" # order it in ascending order
                        }
                    }
                }
            }
        }
    }


search_body = {
    "size": 0,
    "aggs": {
        "histogram_by_date":  {
            "date_histogram": {
                "field": "dateTime",
                "calendar_interval": "hour"
            }
        }
    }
}




search_body = {"size": 0,
      "aggs": {
        "total_orders_per_day_of_week": {
          "terms": {
            "script": {
              "lang": "painless",
              "source": "doc['dateTime'].value.dayOfWeek"
            }
          },
          "aggs": {
            "number_of_weeks": {
              "date_histogram": {
                "field": "dateTime",
                "interval": "week"
              }
            },
            "average_orders_per_day_of_week": {
              "bucket_script": {
                "buckets_path": {
                  "doc_count": "_count",
                  "number_of_weeks": "number_of_weeks._bucket_count"
                },
                "script": "params.doc_count / params.number_of_weeks"
              }
            }
          }
        }
      }
    }

search_body = {"size": 0,
    "aggs": {
        "holidays": {
            "date_range": {
                "field": "dateTime",
                "format": "dd-MM-yyyy",
                "ranges": [
                    { "from": "01-01-2018", "to": "02-01-2018" }, #New Years Day
                    { "from": "30-03-2018", "to": "31-03-2018" }, #Good Friday
                    { "from": "01-04-2018", "to": "03-04-2018" }, #Easter
                    { "from": "27-04-2018", "to": "28-04-2018" }, #Kings Day
                    { "from": "05-05-2018", "to": "06-05-2018" }, #Liberation Day
                    { "from": "10-05-2018", "to": "11-05-2018" }, #Ascension Day
                    { "from": "20-05-2018", "to": "22-05-2018" }, #Pentecost
                    { "from": "25-12-2018", "to": "27-12-2018" }  #Christmas
                ]
            },
            "aggs": { # the aggregation is going over the weekdays per hour, were results are found for
                "orders_per_hour":  {
                    "terms": { "size": 24, # specifying the amount of hours per day = results. Weird enough 0 in the beginning was not sufficient
                        "script": {
                            "lang": "painless",
                            "source": "doc['dateTime'].value.hourOfDay"
                        },
                        "order": {
                            "_key": "asc" # order it in ascending order
                        }
                    },
                    "aggs": {
                        "total_travel_time": {
                            "sum": {
                                "field": "tm" # counting the total time traveled
                            } # for every hour during de week per day
                        }
                    }
                }
            }
        }
    }
}

             "aggs" : {
                "numberOfOccurrences" : {
                    "cardinality": {
                        "script" : {
                            "lang": "painless",
                            "source": "doc['dateTime'].value.dayOfYear"
                        }
                    }
                }
            }

"""

result = es.search(index="dapom_orders", body=search_body)
print(json.dumps(result, indent=4))
# print(json.dumps(result["aggregations"]["statistics_amount"], indent=1))

#es_df = pd.DataFrame(result)
#print(es_df) # print out the DF object's contents
