# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import pymongo
import json
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
from kafka import KafkaProducer

class HardwarezonePipeline:
    def process_item(self, item, spider):
        return item


class RemoveWhiteSpacePipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        
        content = adapter["content"]
        new_content = []
        
        for string in content:
            string = string.replace("\n", " ")
            string = string.replace("\t", " ")
            string = string.strip()
                
            new_content.append(string)
                
        adapter["content"] = new_content
        return item


class RemoveJSONPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        content = adapter["content"]
        new_content = []
        
        for string in content:
            if string.startswith("{") and string.endswith("}"):
                string = ""
            
            new_content.append(string)
        
        adapter["content"] = new_content
        return item


class CombineIntoStringPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        
        content = adapter["content"]
        content = list(filter(("").__ne__, content))
        adapter["content"] = " ".join(content).strip()
        
        return item


class DataValidationPipeline:
    def process_item(self, item, spider):
        valid = True
        
        for data in item:
            if not data:
                valid = False
                raise DropItem("Missing {0}!".format(data))
        
        if valid:
            return item
        

class KafkaPipeline:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'], \
            value_serializer=lambda v:json.dumps(v).encode('utf-8'))
        
    def process_item(self, item, spider):
        valid = True
        for data in item:
            if not data:
                valid = False
                raise DropItem("Missing {0}!".format(data))
        if valid:
            self.producer.send('scrapy-output', dict(item))
        return item