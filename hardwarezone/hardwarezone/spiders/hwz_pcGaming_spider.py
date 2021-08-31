import scrapy
import logging

logger = logging.getLogger('hwz_logger')
class HWZSpider(scrapy.Spider):
    name = 'hardwarezone'
    
    custom_settings = {
        'COLLECTION': 'pc_gaming'
    }
    
    start_urls = [
        'https://forums.hardwarezone.com.sg/forums/pc-gaming.382/'
    ]
    
    def parse(self, response):
        
        for topic_list in response.xpath('//div[has-class("structItemContainer-group js-threadList")]'):
            for topic in topic_list.xpath('div[has-class("structItem structItem--thread js-inlineModContainer")]'):
                topic_page = topic.xpath('div/div[has-class("structItem-title")]/a/@href').get()
                logger.info(topic_page)
                
                yield response.follow(topic_page)
                
        for post in response.xpath('//div[has-class("block-container lbContainer")]/div'):
            yield {
                'topic': response.xpath('//div[has-class("p-title")]/h1/text()').get(),
                'author': post.xpath('article/div/div/section/div/h4/a/text()').get(),
                'content': post.xpath('article/div/div[has-class("message-cell--main")]/div/div/div/article/div/descendant::text()').extract(),
            }
        
        next_page = response.xpath('//a[has-class("pageNav-jump", "pageNav-jump--next")]/@href').get()
        if next_page is not None:
            logger.info(next_page)
            yield response.follow(next_page)
            