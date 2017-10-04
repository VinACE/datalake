import re
from urllib.request import urlopen
from bs4 import BeautifulSoup
html = urlopen('https://apf.org/publications/')
bs = BeautifulSoup(html.read());
include_url='https://apf.org'
links = set()
link_count = 0

blog_posts_tag = bs.find("section", id = "pub-list")
print(blog_posts_tag)
for link_tag in blog_posts_tag.findAll("a", href=re.compile("^(/|.*" + include_url + ")")):
    # print(link_tag)
    if link_tag.attrs['href'] is not None:
                if link_tag.attrs['href'] not in links:
                    if link_tag.attrs['href'].startswith('/'):
                        link = include_url + link_tag.attrs['href']
                    else:
                        link = link_tag.attrs['href']
                    links.add(link)
                    link_count = link_count + 1
print(links)

