from bs4 import BeautifulSoup
import requests
import os

"""
Program to parse html from the state of the union address.
Html is then saved to a text file. (This was a requirement for the project)
"""


def get_list_of_sites():
    MAIN_URL = "http://www.stateoftheunion.onetwothree.net/texts/index.html"
    headers = {'Accept':'*/*','Accept-Charset':'*','Accept-Encoding': '*','Accept-Language': '*','User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36','Referer':'http://www.google.com/' }
    response = requests.get(MAIN_URL,headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')
    list_items = soup.find_all('ul')[1]
    links = []
    for a in list_items.find_all('a',href=True):
        links.append("http://www.stateoftheunion.onetwothree.net/texts/" + a['href'])
    return links

def save_html_files(links, save_to = "C:/Users/anike/Desktop/Education/George Mason University/Sem 4-Fall - 2020/CS 657 - Mining Massive Datasets/Assignments/Assignment 1/speeches/"):
    for link in links:
        print("Writing:",link)
        headers = {'Accept':'*/*','Accept-Charset':'*','Accept-Encoding': '*','Accept-Language': '*','User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (HTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36','Referer':'http://www.google.com/' }
        response = requests.get(link,headers=headers)
        soup = BeautifulSoup(response.content, 'html.parser')
        file_name = link.split("/")[-1].split(".")[0]
        with open(os.path.join(save_to,file_name +".txt"),'w') as file:
            file.write(str(soup.prettify()))

if __name__ == "__main__":
    links = get_list_of_sites()
    save_html_files(links)