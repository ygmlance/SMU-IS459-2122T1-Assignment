# Assignment 1

## Introduction

The web scraper uses __Scrapy__ to crawl and scrape HardwareZone's PC Gaming forum. The post scraped from the forum will be then stored into __MongoDB__.


## Environment Setup

To run the web scraper you will need the following installed on your host machine

1) Python 3 or later
2) MongoDB 5.0 

In absence of __MongoDB__, you can install __Docker__ on your host machine. Within this project folder, there is a `docker-compose.yml` file that can be used to deploy a containerised version of __MongoDB__. 

<br>

To install all the Python libraries required, run the following command
```
pip3 install -r requirements.txt
```


## MongoDB Docker Deployment

To run the containerised version of __MongoDB__, run the following command in the same folder as `docker-compose.yml`
```
docker-compose up -d
```

---
<br>

## Configuring Scrapy

To allow the web scraper to store the post into __MongoDB__, there are a few project level configuration that needs to be set. The settings can be found within `hardwarezone/hardwarezone/settings.py`.

There are 3 specific parameter that needs to be adjusted
- MongoDB Location
- MongoDB Port
- MongoDB Database

If the __MongoDB__ is ran locally and connection port was not changed, both `MONGODB_SERVER` and `MONGODB_PORT` do not need to be changed

`MONGO_DB` denotes the database that should be stored in. By default, it will be named as `hardwarezone`. 

<br>

## Running Scrapy

To start the scraping process, run the command 

```
scrapy runspider hwz_pcGaming_spider.py
```

By running the command, Scrapy will start the scraping __PC Gaming__ gaming forum and will store all the files into __MongoDB__. 