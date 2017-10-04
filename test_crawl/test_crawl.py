from bs4 import BeautifulSoup
import urllib
import requests
from urllib.parse import urlparse

class Crawler(object):
    site_name = ''
    pages = set()
    bulk_data = []
    nrpages = 50
    # bs = BeautifulSoup('http://www.cosmeticsdesign.com//Product-Categories/Skin-Care')

    def __init__(self, site, nrpages):
        self.site = site
        self.nrpages = nrpages


    # read the content of a page into BeautifulSoup
    def read_page(self, url):
        bs = BeautifulSoup('')

        try:
            print("read_page: scraping url ", url)
            print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")

            html = urllib.request.urlopen(url)
            print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
            print(html)
            print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
            bs = BeautifulSoup(html.read(), "lxml")
            print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")

            [script.decompose() for script in bs("script")]
        except:
            print("Scrape: could not open url ", url)
        return bs

if __name__=="__main__":

    c = Crawler('http://www.cosmeticsdesign.com/Product-Categories/Skin-Care', 50)
    cc = c.read_page('http://www.cosmeticsdesign.com/Product-Categories/Skin-Care')
    print(cc)