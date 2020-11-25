import pickle
from google.cloud import storage
from google.cloud import bigquery
from collections import defaultdict

# Markov graph
markov_graph = {}

# Start word weights for new tweets
start_words = {}

# build the ditionary and get start word weights
def create_markov_graph():
    # The source data is already prepared in BigQuery. Read it into dictionary format.
    query_sql = "SELECT * FROM `tanelis.markov_chain.markov_graph_view`"

    client = bigquery.Client()
    query_job = client.query(query_sql)
    results = query_job.result()

    for row in results:
        current_word = row["current_word"]
        start_count = row["first_word_count"]
        next_array = row["next"]

        markov_graph[current_word] = {}

        # Get the next words data for the markov graph
        for obj in next_array:
            next_word = obj["word"]
            next_word_weight = obj["weight"]
            next_word_stop = obj["stop"] # if the word is a "stop word" that leads to a dead end
            next_word_end_probability = obj["end_probability"] # a probability for each word on how often they should end the tweet once min words are reached

            markov_graph[current_word][next_word] = [
                next_word_weight, next_word_stop, next_word_end_probability]

        # Get the start word weights for when the tweet is generated with a random start word
        start_words[current_word] = start_count


def store_pickle(bucket, folder, file_name, dict_data):
    file_path = '{}/{}.pickle'.format(folder, file_name)
    print('Storing file to: {}'.format(file_path))
    blob = bucket.blob(file_path)
    blob.upload_from_string(pickle.dumps(dict_data))


create_markov_graph()

storage_client = storage.Client()
bucket = storage_client.bucket('markov_generator')
folder = 'all_meps'

# Save the markov graph
store_pickle(bucket, folder, 'markov_graph', markov_graph)

# Save the start words
store_pickle(bucket, folder, 'start_words', start_words)