"""
Final Assignment Data Analysis and Programming for Operations Management
Code developed by:      Martijn de Jonge - Pirozzi
Student number:         S4147286

Goal of the code:
Iterating through a certain timeframe from the dataset
with a 100% self-written mix&match function to combine orders that are within certain boundaries like order time, travel time etc.

"""
%reset -f


# loading modules that we use
from datetime import datetime
from datetime import timedelta
from colorama import Fore, Style
import sys, os

import json
import pandas as pd
from pandas import json_normalize
from pandasticsearch import Select
import numpy as np

from elasticsearch_dsl import Search
from elasticsearch import Elasticsearch

es = Elasticsearch([{'host':'127.0.0.1', 'port': 9200}])

def mix_and_match_orders():
    search_body = {
            "size": 5000,
           "sort" : [
            { "dateTime" : {"order" : "asc"}}],
            "query": {
                "bool": {
                "filter": {
                    "range": {
                        "dateTime": {
                            "format": "dd-MM-yyyy HH:mm:ss",
                            "gte": "01-01-2018 19:00:00",
                            "lte": "01-01-2018 20:00:00"
                        }
                    }
                }
            }
        }
    }
    result_orders = es.search(index="dapom_orders", body=search_body)

    # print(json.dumps(result, indent=1))


    A_restaurant = []
    A_postcode_restaurant = []
    A_postcode_destination = []
    A_time = []
    A_combined = []


    doc_orders = result_orders['hits']['hits']

    for order in doc_orders:
        A_restaurant.append(order["_source"]["restaurant"])
        A_postcode_restaurant.append(order["_source"]["postcodeFrom"])
        A_postcode_destination.append(order["_source"]["postcodeTo"])
        A_time.append(datetime.strptime(order["_source"]["dateTime"], "%Y-%m-%d %H:%M:%S"))
        A_combined.append(False)

    x = 5 # max amount of minutes between ordering
    y = 6 # max amount of minutes driving between restaurants
    z = 6 # max amount of minutes driving between destinations

    for i in range(len(A_postcode_restaurant)-1):
        if not A_combined[i]:
            for j in range(i+1, len(A_postcode_restaurant)):
                if not A_combined[j]:
                    if ((A_time[j] - timedelta(weeks=0, days=0, hours=0, minutes=x, seconds=0)) <= A_time[i]):
                        print(Fore.YELLOW + 'PASSED FIRST CHECK: ORDERS ARE PLACED WITHIN RANGE OF', x, 'MINUTES OF EACH OTHER:')
                        print('Postal code', A_postcode_restaurant[i], "and ", A_postcode_restaurant[j],
                        'are in the range of ordering within', x,
                        'minutes of each other. The actual time between orders is', ((A_time[j] - A_time[i])))
                        print('Order A was placed at', A_time[i], 'and order B at', A_time[j],  Style.RESET_ALL)

                        body_post_a = {
                                "query": {
                                      "bool": {
                                        "must": [
                                            {
                                                "match": {
                                                   "src": A_postcode_restaurant[i] # postal code restaurant order A
                                                }
                                            },
                                            {
                                                "match": {
                                                    "dest": A_postcode_restaurant[j] # postal code restaurant order B
                                                }
                                            }
                                        ]
                                    }
                                }
                            }

                        result_post_a = es.search(index="post_distances", body=body_post_a)
                        # print(json.dumps(result_post, indent=1))
                        doc_post_a = result_post_a['hits']['hits']


                        B_postcode_restaurant = []
                        B_postcode_destination = []
                        B_time = []

                        for postalcode in doc_post_a:
                            B_postcode_restaurant.append(postalcode["_source"]["src"])
                            B_postcode_destination.append(postalcode["_source"]["dest"])
                            B_time.append(postalcode["_source"]["seconds"])

                        for i in range(len(B_time)):
                            if ((int(float(B_time[i])) / 60) <= y):

                                print(Fore.GREEN + 'PASSED SECOND CHECK:')
                                # starting to look up traveling time between destinations
                                body_post_b = {
                                        "query": {
                                              "bool": {
                                                "must": [
                                                    {
                                                        "match": {
                                                           "src": A_postcode_destination[i] # postal code customer A
                                                        }
                                                    },
                                                    {
                                                        "match": {
                                                            "dest": A_postcode_destination[j] # postal code customer B
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    }

                                result_post_b = es.search(index="post_distances", body=body_post_b)
                                # print(json.dumps(result_post, indent=1))
                                doc_post_b = result_post_b['hits']['hits']

                                C_postcode_dest1 = []
                                C_postcode_dest2 = []
                                C_time = []


                                for postalcode in doc_post_b:
                                    C_postcode_dest1.append(postalcode["_source"]["src"])
                                    C_postcode_dest2.append(postalcode["_source"]["dest"])
                                    C_time.append(postalcode["_source"]["seconds"])

                                if not A_combined[i] & A_combined[j]:
                                        # int((int(float(C_time[i])) / 60) - z) checking
                                        if ((int(float(C_time[i])) / 60) <= z):
                                            A_combined[i] = True
                                            A_combined[j] = True
                                            body_travel_time1 = {
                                                    "query": {
                                                          "bool": {
                                                            "must": [
                                                                {
                                                                    "match": {
                                                                       "src": B_postcode_destination[i] # postal code restaurant B
                                                                    }
                                                                },
                                                                {
                                                                    "match": {
                                                                        "dest": C_postcode_dest1[i] # postal code customer A
                                                                    }
                                                                }
                                                            ]
                                                        }
                                                    }
                                                }

                                            body_travel_time2 = {
                                                    "query": {
                                                          "bool": {
                                                            "must": [
                                                                {
                                                                    "match": {
                                                                       "src": C_postcode_dest1[i] # postal code customer A
                                                                    }
                                                                },
                                                                {
                                                                    "match": {
                                                                        "dest": C_postcode_dest2[i] # postal code customer B
                                                                    }
                                                                }
                                                            ]
                                                        }
                                                    }
                                                }
                                            body_travel_time3 = {
                                                    "query": {
                                                          "bool": {
                                                            "must": [
                                                                {
                                                                    "match": {
                                                                       "src": C_postcode_dest2[i] # postal code customer AC_postcode_dest1
                                                                    }
                                                                },
                                                                {
                                                                    "match": {
                                                                        "dest": B_postcode_destination[i] # postal code customer B
                                                                    }
                                                                }
                                                            ]
                                                        }
                                                    }
                                                }

                                            result_travel_time1 = es.search(index="post_distances", body=body_travel_time1)
                                            doc_travel_time1 = result_travel_time1['hits']['hits']

                                            result_travel_time2 = es.search(index="post_distances", body=body_travel_time2)
                                            doc_travel_time2 = result_travel_time2['hits']['hits']

                                            result_travel_time3 = es.search(index="post_distances", body=body_travel_time3)
                                            doc_travel_time3 = result_travel_time3['hits']['hits']


                                            C_rest1 = A_postcode_restaurant[i]
                                            C_rest2 = B_postcode_destination[i]
                                            C_drop1 = C_postcode_dest1[i]
                                            C_drop2 = C_postcode_dest2[i]


                                            C_set = [C_rest1, C_rest2, C_drop1, C_drop2]


                                            time_rest1_to_rest2 = B_time[i]
                                            time_rest2_to_dest1 = []
                                            time_dest1_to_dest2 = []
                                            time_dest2_to_rest1 = []

                                            for set in doc_travel_time1:
                                                time_rest2_to_dest1.append(set["_source"]["seconds"])

                                            for set in doc_travel_time2:
                                                time_dest1_to_dest2.append(set["_source"]["seconds"])

                                            for set in doc_travel_time3:
                                                time_dest2_to_rest1.append(set["_source"]["seconds"])

                                            print(Fore.GREEN + 'PASSED THIRD CHECK: WE CAN COMBINE THE ORDERS')
                                            print('Start at restaurant', C_rest1, 'then travel ' + str(int(float(B_time[i])) / 60),
                                            'minutes to second restaurant at', C_rest2,
                                            '\nthen travel ' + str(int(float(time_rest2_to_dest1[i])) / 60), 'to customer one', C_drop1,
                                            'then travel ' + str(int(float(time_dest1_to_dest2[i])) / 60),
                                            'to customer two',  C_drop2 + Style.RESET_ALL)
                                            print('Total travel time is '
                                            + str((int(float(time_rest1_to_rest2))
                                            + int(float(time_rest2_to_dest1[i]))
                                            + int(float(time_dest1_to_dest2[i]))
                                            + int(float(time_dest2_to_rest1[i])))
                                            / 60), 'minutes')
                                            if A_combined[i] & A_combined[j]:
                                                C_set = "Restaurant 1: " + C_rest1 + " ,Restaurant 2: " + C_rest2 + " ,Drop-off 1: " + C_drop1 + " ,Drop-off 2: " + C_drop2
                                                print(C_set, '\n\n')

                                        else:
                                            print(Fore.RED + 'DID NOT PASS THIRD CHECK:', (int(float(C_time[i])) / 60), 'minutes real travel time between destinations.', "\n\n")
"""
                            else:
                                A_combined[i] = False
                                A_combined[j] = False
                                print(Fore.RED + 'DID NOT PASS SECOND CHECK: CAN NOT COMBINE ORDERS')
                                print('Distance too big. not combinable:')
                                print('Postal code is ', B_postcode_restaurant[i], 'goes to', B_postcode_destination[i],
                                'and time between restaurants is ' + str(int(float(B_time[i])) / 60), '\n\n' + Style.RESET_ALL)

            else:
                A_combined[i] = False
                A_combined[j] = False
                print('DID NOT PASS FIRST CHECK: CAN NOT COMBINE ORDERS')
                print('The orders of postal code', A_postcode_restaurant[i], 'and postal code', A_postcode_restaurant[j],
                'are not ordered within', x, 'minutes of each other.')
                print("\n\n")
"""

mix_and_match_orders()
