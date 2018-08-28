import glob
import os
import json
import time
from datetime import datetime, timedelta

spark_result_dir   = '/home/gabriel/dev/bigdata/news-visualization-with-spark/output_test/'
agg_output_dir     = '/home/gabriel/dev/bigdata/news-visualization-with-spark/agg_test/'
agg_control_file   = 'control_test.json'
agg_result_file    = 'entities_test.json'
sleep_time_seconds = 60
timedelta_accepted = timedelta(0, 1800) # second argument is seconds

def get_window_datetime(window_start):
    last_colon_idx  = window_start.rfind(':')
    return datetime.strptime(window_start[:last_colon_idx]+window_start[last_colon_idx+1:], '%Y-%m-%dT%H:%M:%S.%f%z')

def update_control(control, spark_result):
    country = spark_result['region']

    if country not in control['countries']:
        control['countries'][country] = {}

    window_start = spark_result['window']['start']

    window_datetime = get_window_datetime(window_start)
    if 'most_recent_window' not in control:
        control['most_recent_window'] = window_start
    else:
        if window_datetime > get_window_datetime(control['most_recent_window']):
            control['most_recent_window'] = window_start

    if window_start not in control['countries'][country]:
        control['countries'][country][window_start] = {}

    entity = spark_result['entity']

    if entity not in control['countries'][country][window_start]:
        control['countries'][country][window_start][entity] = {}

    if 'count' not in control['countries'][country][window_start][entity]:
        control['countries'][country][window_start][entity]['count'] = 0

    if 'sentiments' not in control['countries'][country][window_start][entity]:
        control['countries'][country][window_start][entity]['sentiments'] = {}

    sentiments = spark_result['collect_list(sentiment)']
    urls       = spark_result['collect_list(url)']

    if len(sentiments) == len(urls):        
        for i in range(len(urls)):
            if urls[i] not in control['countries'][country][window_start][entity]['sentiments']:
                control['countries'][country][window_start][entity]['count'] += 1
                control['countries'][country][window_start][entity]['sentiments'][urls[i]] = []
            
            control['countries'][country][window_start][entity]['sentiments'][urls[i]].append(float(sentiments[i]))

def generate_result(control):
    result = []
    
    for country in control['countries']:
        country_dict = {'country': country, 'entities':[]}

        entities_data = {}

        for window_start in control['countries'][country]:
            window_data = control['countries'][country][window_start]

            for entity in window_data:
                if entity not in entities_data:
                    entities_data[entity] = {'count': 0, 'sentiments': {}}

                for url in window_data[entity]['sentiments']:
                    if url not in entities_data[entity]['sentiments']:
                        entities_data[entity]['count'] += 1
                        entities_data[entity]['sentiments'][url] = []
                    
                    entities_data[entity]['sentiments'][url] += window_data[entity]['sentiments'][url]
        
        for entity in entities_data:
            entity_dict = {'name': entity, 'value': entities_data[entity]['count'], 'url_sentiments': []}
            
            sentiments  = []

            for url in entities_data[entity]['sentiments']:
                url_sentiments    = entities_data[entity]['sentiments'][url]
                url_avg_sentiment = sum(url_sentiments)/len(url_sentiments)
                
                entity_dict['url_sentiments'].append({'url': url, 'sentiment': url_avg_sentiment})

                sentiments.append(url_avg_sentiment)

            entity_dict['avg_sentiment'] = sum(sentiments) / len(sentiments)

            country_dict['entities'].append(entity_dict)
        
        result.append(country_dict)

    return result

# read existing control object, or create empty one
control = {'countries':{}}
agg_control_fullpath = os.path.join(agg_output_dir, agg_control_file)
if os.path.isfile(agg_control_fullpath):
    with open(agg_control_fullpath, 'r') as f:
        control = json.loads(f.read())

# run indefinitely
while True:
    # list all files in spark output directory
    spark_result_files = glob.glob(os.path.join(spark_result_dir, '*.json'))

    for spark_result_file in spark_result_files:
        print("Processing file '{}' ...".format(spark_result_file))
        try:
            with open(spark_result_file, 'r') as f:
                # process the json in each line and update the control object with its data
                for line in f:
                    spark_result = json.loads(line)
                    update_control(control, spark_result)
            
            # delete file after it's been processed
            os.remove(spark_result_file)
        except Exception as e:
            print(e)
    
    # remove windows that are too old from each country
    countries_to_remove = []
    for country in control['countries']:
        keys_to_remove = []
        for window_start in control['countries'][country]:
            window_datetime = get_window_datetime(window_start)
            if (get_window_datetime(control['most_recent_window']) - window_datetime) > timedelta_accepted:
                keys_to_remove.append(window_start)
        for key in keys_to_remove:
            control['countries'][country].pop(key, None)
        if len(control['countries'][country]) == 0:
            countries_to_remove.append(country)
    for country in countries_to_remove:
        control['countries'].pop(country, None)

    # write updated control object to file
    print('Writing updated control file...')
    with open(agg_control_fullpath, 'w') as out:
        out.write(json.dumps(control, indent=4))

    # generate result object and write it to file for the visualization
    print('Generating result...')
    result = generate_result(control)
    with open(os.path.join(agg_output_dir, agg_result_file), 'w') as out:
        out.write(json.dumps(result, indent=4))
    
    print("Sleeping for {} seconds...".format(sleep_time_seconds))
    time.sleep(sleep_time_seconds)