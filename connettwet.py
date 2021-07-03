import jsonlines, json
# Stores hydrated tweets here as jsonl objects
# Contains one json object per line
output_json_filename = output_filename[:output_filename.index(".")] + ".txt"
ids = []
with open(final_tweet_ids_filename, "r") as ids_file:
    ids = ids_file.read().split()
hydrated_tweets = []
ids_to_hydrate = set(ids)

# Looks at the output file for already hydrated tweets
if os.path.isfile(output_json_filename):
    with jsonlines.open(output_json_filename, "r") as reader:
        for i in reader.iter(type=dict, skip_invalid=True):
            # These tweets have already been hydrated. So remove them from ids_to_hydrate
            hydrated_tweets.append(i)
            ids_to_hydrate.remove(i["id_str"])
print("Total IDs: " + str(len(ids)) + ", IDs to hydrate: " + str(len(ids_to_hydrate)))
print("Hydrated: " + str(len(hydrated_tweets)))

count = len(hydrated_tweets)
start_index = count # The index from where tweets haven't been saved to the output_json_file
# Stores hydrated tweets to output_json_file every num_save iterations.
num_save  = 1000

# Now, use twarc and start hydrating
for tweet in t.hydrate(ids_to_hydrate):
    hydrated_tweets.append(tweet)
    count += 1
    # If num_save iterations have passed,
    if (count % num_save) == 0:
        # Open the output file
        # NOTE: Even if the code stops during IO, only tweets from the current iteration are lost.
        # Older tweets are preserved as the file is written in append mode.
        with jsonlines.open(output_json_filename, "a") as writer:
            print("Started IO")
            # Now write the tweets from start_index. The other tweets don't have to be written
            # as they were already written in a previous iteration or run.
            for hydrated_tweet in hydrated_tweets[start_index:]:
                writer.write(hydrated_tweet)
            print("Finished IO")
        print("Saved " + str(count) + " hydrated tweets.")
        # Now, since everything has been written. Reset start_index
        start_index = count
# There might be tweets unwritten in the last iteration if the count is not a multiple of num_tweets.
# In that case, just write out the remainder of tweets.
if count != start_index:
    print("Here with start_index", start_index)
    with jsonlines.open(output_json_filename, "a") as writer:
        for hydrated_tweet in hydrated_tweets[start_index:]:
           writer.write(hydrated_tweet)
