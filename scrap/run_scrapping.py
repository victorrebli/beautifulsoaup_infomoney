import urllib
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime
import pickle
from multiprocessing import Pool, Manager
import time
from google.cloud import storage
import os

storage_client = storage.Client()

filename = 'site1.json'
bucket = storage_client.get_bucket("infomoney")
if filename:
    blob = bucket.get_blob(f'org_site/{filename}')
    datastore = json.loads(blob.download_as_string())


hdr = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
        'Accept-Encoding': 'none',
        'Accept-Language': 'en-US,en;q=0.8',
        'Connection': 'keep-alive'}


def testando(_key, dici):
    
    base = 'https://www.infomoney.com.br/'
    first_url = base + _key
    print(f'Fetching: {first_url}')
    
    try:
        request = urllib.request.Request(first_url,headers=hdr)
        html = urllib.request.urlopen(request)
    except:
        print(f'Erro na request da pagina - {base}')    
    try:          
        bs = BeautifulSoup(html, 'lxml')
    except:
        print(f'Erro no BeatifulSoup1')

    try:
        dc = bs.find('div', id="infiniteScroll")
    except:
        print(f'Erro no BeatifulSoup2')

    try:
        articles = dc.findAll("div", {"class": "row py-3 item"})
    except:
        print(f'Erro no BeatifulSoup3')

    for _article in articles: 
        try:
            _idd = _article['id']
        except:
            _idd = 'no_id'
        try:
           _link = _article.find('span', class_='hl-title hl-title-2').find('a', href=True)['href']
        except:
           _link = 'no_link'

        if _idd != 'no_id':
            if _idd not in dici.keys():
                dici[_idd] = {'link': _link,'present': [_key]}

            else:
                print(f'data_id duplicated: {_idd}')
                dici[_idd]['present'].append(_key)        


def metid2(dici, processes=4):
    
    base = 'https://www.infomoney.com.br/'
    pool = Pool(processes=processes)
    [pool.apply_async(testando, args=(_key, dici)) for _key in datastore[base]]
    pool.close()
    pool.join()
    print(f'Finalizando')


def funcao_marota(key, dici, idx, tam):
    if idx % 5 == 0:
        print(f'fecthing: {idx} of {tam}')
     
    _full_link = dici[key]['link']  

    try:
        request = urllib.request.Request(_full_link,headers=hdr)
        html = urllib.request.urlopen(request)
    except:
        print(f'Erro na request da pagina - {_full_link}')
        return 'None'
    try:          
        bs = BeautifulSoup(html, 'lxml')
    except:
        print(f'Erro no BeatifulSoup da pagina - {_full_link}')
        return 'None'
     
    try:
        dc_header = bs.find('div', class_="col post-header border-b my-5 px-0 pb-5")
    except:
         print('Erro obtencao dc_header')
             
    try:
        _title = dc_header.find('h1', class_='page-title-1').text
    except:
        _title = 'no_title'
        
    try:
        _fonte = dc_header.find('span', class_="author-name").text   
    except:
        _fonte = 'no_fonte'
        
    try:
        _data = dc_header.find('time', class_='entry-date published').text
    except:
        _data ='no_date'
    _data_scrap = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    
    
    try:
        _bs = bs.find('div', class_='col-md-9 col-lg-8 col-xl-6 m-sm-auto m-lg-0 article-content')
    except:
        print('erro na obtencao do artigo')
        
        
    try:
        _text = _bs.find_all('p')
    except:
        print('erro em obter o texto - p')
        return 'None'

    _text_final_interm = []
    for text in _text:
        _text_final_interm.append(text.text)
    _text_final = "/n".join(_text_final_interm)

    return key, _title, _data, _data_scrap, _text_final


def metid(dici, processes=4):
    pool = Pool(processes=processes)           
    def aggregator(res): 
        if res != 'None':
            dici[res[0]]['title'] = res[1]
            dici[res[0]]['data_article'] = res[2]
            dici[res[0]]['data_scrap'] = res[3]
            dici[res[0]]['text'] = res[4]

    tam = len(dici.keys())
    [pool.apply_async(funcao_marota, args=(_key, dici, idx, tam), callback=aggregator) for idx, _key in enumerate(dici.keys())]
    pool.close()
    pool.join()
    print(f'Finalizando')

    return dici

def _save_file(dici_final):
    date_file = datetime.now().strftime("%d-%m-%Y-%H:%M")
    blob = bucket.blob(f'write_p/{date_file}.pickle')
    blob.upload_from_string(
        data=json.dumps(dici_final),
        content_type='application/json'
       )
    print('Saved') 

     
def main(request):
    manager = Manager()
    dici = manager.dict()

    inicio = time.asctime(time.localtime(time.time()))
    metid2(dici, processes=2)
    dici_t = dici.copy()
    dc = metid(dici_t, processes=2)
    _save_file(dc)
    fim = time.asctime(time.localtime(time.time()))
    print(f'Inicio: {inicio} - Fim: {fim}')
    return {'Sucesso': 'True'}
         