# import libraries for the pipeline.
import requests
import pandas as pd
import json
from pandas import json_normalize
import time 
from datetime import datetime, timedelta

API_KEY = 'Enter here'

def get_URI(query:str, page_num:str, date:str, API_KEY:str) -> str:
	"""# obtain the URI for access to articles for a given query, page number, and date"""

	# Append the query to the URI
	URI = f'https://api.nytimes.com/svc/search/v2/articlesearch.json?q={query}'

	# Add page number and date to the URI
	URI = URI + f'&page={page_num}&begin_date={date}&end_date={date}'

	# Add an API Key to access the information
	URI = URI + f'&api-key={API_KEY}'

	return URI


""" Here is where the dataframe will be collected in order to store all the information """
# Create the dataframe
df = pd.DataFrame()

# Get the current date

#tomorrow = datetime.now() + timedelta(1)
news_date = datetime.now().strftime('%Y%m%d')
#print(tomorrow, ", ", news_date)

# Cycle through the pages to collect all data
page_num = 1
while True:
	# Grab the URI needed for the article related to "Climate Change" from newest to oldest
	URI = get_URI(query='climate', page_num=str(page_num), date=news_date, API_KEY=API_KEY)

	# Make a request with the URI
	response = requests.get(URI)

	# Collect the data in JSON format
	data = response.json()

	# Convert data to a dataframe !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	df_request = json_normalize(data['response'], record_path=['docs'])

	# End loop if no other articles are available
	if df_request.empty:
		break

	# Append df_request to the dataframe
	df = pd.concat([df, df_request])

	# Pause to stay within the limits of the number of requests (10)
	time.sleep(6)

	# Go to the next page
	page_num += 1


df.info()

input('Press enter to quit.')

################################
# Now we clean up the data to stage it for housing.

if len(df) > 0:
	if len(df['_id'].unique()) < len(df):
		print('There are duplicates in the dataframe.')
		df.drop_duplicates('_id', keep='first')

	# Search for and replace articles with missing headlines.
	if df['headline.main'].isnull().any():
		print('There are missing headlines in the dataframe.')
		df = df[df['headline.main'].isnull() == False]

	# Filter out the op-eds.
	df = df[df['type_of_material'] != 'op-ed']

	# We only want the headline, pub date, author and url.
	df = df[['headline.main', 'pub_date', 'byline.original', 'web_url']]

	# Rename the columns.
	df.columns = ['headline', 'date', 'author', 'url']

	print(df.head())

else:
	print("No articles found.")
