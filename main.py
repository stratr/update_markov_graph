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
        start_weight = row["start_probability"]
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
        start_words[current_word] = start_weight


create_markov_graph()

storage_client = storage.Client()
bucket = storage_client.bucket('markov_generator')
folder = 'all_meps'

# Save the markov graph
markov_blob = bucket.blob('all_meps/markov_graph.pickle')
markov_blob.upload_from_string(pickle.dumps(markov_graph))

# Save the start words
start_blob = bucket.blob('all_meps/start_words.pickle')
start_blob.upload_from_string(pickle.dumps(start_words))


# pickle_in = blob.download_as_string()
# my_dictionary = pickle.loads(pickle_in)
# print(my_dictionary)
# print(type(my_dictionary))