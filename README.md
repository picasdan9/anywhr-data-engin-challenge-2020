# anywhr-data-engin-challenge-2020
This repository is part of my submission for Anywhr Data Engineering Challenge

Task: Many travellers are often interested in visiting famous spots where movies were shot. How can we build a database of such famous spots?

## Installation
Install neccessary pacakges.

```
pip install dnspython requests re bs4 pymongo pandas numpy
```

## Directories
* ``blueprint.docx`` - contains the task and my approach to solving it
* ``exploratory_script.ipynb`` - shows how I went about writing the script
* ``script.py`` - generates movieIDs.csv and movie_spots.csv
* ``urls.txt`` - contains the URLs from which script.py scrapes
* ``movieIDs.csv`` - contains titles of movies and their associated IDs
* ``movie_spots.csv`` - contains infomration about locations where films were shot

## MongoDB Database query
The *movies* online database contains 2 collections
* *locations* collection - equivalent to ``movie_spots.csv``
* *movieIDs* collection - equivalent to ``movieIDs.csv``

Python code
```
mongoDBpass_guest = '0ra16Sa1wUpCdHrk'
client = MongoClient('mongodb+srv://guest:{}@movie-spots-odvvz.mongodb.net/test?retryWrites=true&w=majority'.format(mongoDBpass_guest))
db = client.movies
```

## Built With
Python
