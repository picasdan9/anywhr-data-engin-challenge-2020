import requests
import urllib.request
import time
import re
from bs4 import BeautifulSoup
from pymongo import MongoClient
import pandas as pd

mongoDBpass = '<INSERT MONGODB USER PASS HERE.'
movieID_table = []

def get_soup(url):
  response = requests.get(url, headers = {'User-Agent': 'Mozilla/5.0'})
  soup = BeautifulSoup(response.text, "html.parser")
  return soup

def create_movieID(soup):
  title = soup.find("h1").text
  movieID = hash(title)
  movieID_table.append({"title": title, "movieID": movieID})
  return movieID

# get_loc_desc(soup) returns a pandas DataFrame of the film's locations, scene descriptions, address (if have) and city
def get_loc_desc(soup):
  name_elm = soup.findAll("a", href=re.compile("th$"))
  names = [re.sub(r'[^A-Za-z0-9]+', ' ', i.text).strip() for i in name_elm]
    
  def remove_tail(name):
    try: 
        if re.search(r"^\d", name):
            return name
        else: return name[:re.search(r"\d",name).start()]
    except:
        return name
  
  def get_address(name):
    try: 
        return name[re.search(r"\d", name).start():]
    except:
        return None

  def get_city(elm):
    try: 
        loc = elm.parent.parent.find("li", class_="name").text
        return loc[:loc.find(' locations')]
    except:
        return None
    
  names_without_address = [remove_tail(name).strip() for name in names]
  addresses = [get_address(name) for name in names]
  cities = [get_city(elm) for elm in name_elm]

  desc_elm = soup.findAll("span", class_="slant")
  descs = [re.sub(r'[^a-zA-Z0-9]+', ' ', i.text).strip() for i in desc_elm]

  location_desc = [{"name":name, "desc":desc, "address": address, "city": city} 
                   for name, desc, address, city in zip(names_without_address, descs, addresses, cities)]

  return location_desc

# get_loc_address(soup) returns a pandas DataFrame of the film's location with address with postal code
def get_loc_address(soup):
  address_elm_list = [elm.findAll("span", text = ("Visit:", "Shop at:", "Dine at:", "Catch a movie at:", "Drink at:"))
                      for elm in soup.find_all("div", class_="locnbox-960")]

  def get_name_address_city(soup):
      try:
          name = soup.find("a").text
      except:
          name = None
      try:
          address = soup.find("span", class_="name").text
      except:
          address = None
      try:
          city = soup.parent.parent.find("h2").text
      except:
          city = None
      return {"name": name, "address": address, "city": city}
          
  location_address_list = [[get_name_address_city(elm.parent) for elm in elm_list] for elm_list in address_elm_list]

  location_address = []
  for addresses in location_address_list:
      for address in addresses:
          location_address.append(address)
  
  return location_address

# get_movie_spots(url) returns the merged pandas DataFrame from 
# the  DataFrames obtained from get_losc_desc(soup) and get_loc_address(soup)
def get_movie_spots(url):
  soup = get_soup(url)

  df_address = pd.DataFrame(get_loc_address(soup)).dropna()

  desc = get_loc_desc(soup)
  if not desc:
    df_merged = df_address
  else: 
    df_desc = pd.DataFrame(get_loc_desc(soup))
    
    df_merged = df_desc.merge(df_address, how="outer", left_on="name", right_on="name")

    df_merged['address'] = df_merged.apply((lambda row : row['address_y'] 
                                            if isinstance(row['address_y'], str) 
                                            else row['address_x']), axis=1)
    df_merged.drop(['address_x', 'address_y'], axis=1, inplace=True)

    df_merged['city'] = df_merged.apply((lambda row : row['city_x'] 
                                            if isinstance(row['city_x'], str) 
                                            else row['city_y']), axis=1)
    df_merged.drop(['city_x', 'city_y'], axis=1, inplace=True)
  
  df_merged['movieID'] = create_movieID(soup)

  return df_merged


def main():
  # import the urls of the film location pages
  with open('urls.txt', 'r') as f:
    url_list = f.read().split('\n')
  
  # accumulate all the location entires into a single DataFrame and export it
  movie_spots = pd.concat([get_movie_spots(url) for url in url_list])
  movie_spots.to_csv("movie_spots.csv", index=False)

  # export the movieIDs
  df_movieIDs = pd.DataFrame(movieID_table)
  df_movieIDs.to_csv("movieIDs.csv", index=False)

  # insert the tables to mongoDB 'movies' database
  client = MongoClient('mongodb+srv://picasdan:{}@movie-spots-odvvz.mongodb.net/test?retryWrites=true&w=majority'.format(mongoDBpass))
  db = client.movies
  locations = db.locations
  locations.insert_many(movie_spots.to_dict('records'))
  movieIDs = db.movieIDs
  movieIDs.insert_many(movieID_table)
  
main()