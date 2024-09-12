# Script - Web Scraper - YouTube


# Importar Bibliotecas
import csv
import pandas as pd
import os
import re
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Inicializar WebDriver com argumento --headless (serve para que a execução do código rode em segundo plano, não abrindo as guias do navegador na tela)
def init_webdriver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

# Inicializar função para coletar dados da guia 'About' do Canal
def get_about(channel_url):
    driver = init_webdriver()
    try:
        driver.get(channel_url)
        time.sleep(6)
        
        try:
            subscribers = driver.find_element(By.XPATH, '//*[@id="additional-info-container"]/table/tbody/tr[4]/td[2]').text
            views = driver.find_element(By.XPATH, '//*[@id="additional-info-container"]/table/tbody/tr[6]/td[2]').text
            videos = driver.find_element(By.XPATH, '//*[@id="additional-info-container"]/table/tbody/tr[5]/td[2]').text
            data = driver.find_element(By.XPATH, '//*[@id="additional-info-container"]/table/tbody/tr[7]/td[2]/yt-attributed-string/span/span').text
        except Exception as e:
            print(f"Erro ao obter dados do canal: {e}")
            subscribers = views = videos = data = "N/A"
        
        return {
            "channel_url": channel_url,
            "subscribers": subscribers,
            "views": views,
            "videos": videos,
            "data": data
        }
    finally:
        driver.quit()

# Funções para tratamento de dados coletados em About. Esses dados são salvos em Lifetime.csv

def tratar_data_lifetime(file_path):
    meses = {
        'jan.': '01', 'fev.': '02', 'mar.': '03', 'abr.': '04', 'mai.': '05', 'jun.': '06',
        'jul.': '07', 'ago.': '08', 'set.': '09', 'out.': '10', 'nov.': '11', 'dez.': '12'
    }
    df = pd.read_csv(file_path)
    if 'data' in df.columns:
        df['data'] = df['data'].str.replace('Inscreveu-se em ', '', regex=False)
        df['data'] = df['data'].str.replace('de ', '', regex=False)
        for mes_abrev, mes_num in meses.items():
            df['data'] = df['data'].str.replace(mes_abrev, mes_num, regex=False)
        df['data'] = df['data'].str.replace('.', '', regex=False)
        def converter_para_dias_passados(data):
            try:
                data_formatada = datetime.strptime(data, '%d %m %Y')
                dias_passados = (datetime.now() - data_formatada).days
                return dias_passados
            except ValueError:
                return None
        
        df['data'] = df['data'].apply(converter_para_dias_passados)
        df.to_csv(file_path, index=False)
        return df[['data']]
    else:
        return "A coluna 'data' não existe no arquivo CSV."
    
def formatacao_lifetime(file_path):
    df = pd.read_csv(file_path)
    new_data = []

    for index, row in df.iterrows():
        channel_id = index + 1
        channel_name = row['channel_url'].split('@')[-1]
        days_on_youtube = int(row['data'])
        total_views = int(row['views'])
        views_per_day = total_views / days_on_youtube
        total_subscribers = int(row['subscribers'])
        subscribers_per_day = total_subscribers / days_on_youtube
        total_uploads = int(row['videos'])
        uploads_per_month = total_uploads / (days_on_youtube / 30)
        column1 = 0
        new_data.append({
            'Channel Id': channel_id,
            'Channel Name': channel_name,
            'Days on YouTube': days_on_youtube,
            'Total Views': total_views,
            'Views per Day': views_per_day,
            'Total Subscribers': total_subscribers,
            'Subscribers per Day': subscribers_per_day,
            'Total Uploads': total_uploads,
            'Uploads per Month': uploads_per_month,
            'Column1': column1
        })
    new_df = pd.DataFrame(new_data)
    new_df.to_csv('Lifetime.csv', index=False)
    
def lifetime_remove():
    df = pd.read_csv('Lifetime.csv')
    df['channel_url'] = df['channel_url'].str.replace('/about', '', regex=False)
    df['subscribers'] = df['subscribers'].str.replace(' de inscritos', '', regex=False)
    df['subscribers'] = df['subscribers'].str.replace(' inscritos', '', regex=False)
    df['views'] = df['views'].str.replace(' visualizações', '', regex=False)
    df['videos'] = df['videos'].str.replace(' vídeos', '', regex=False)
    df.to_csv('Lifetime.csv', index=False)
    
def tratar_subs(file_path):
    def converter_valor(valor):
        if 'mil' in valor:
            return int(float(valor.replace(' mil', '').replace(',', '.')) * 1000)
        elif 'mi' in valor:
            return int(float(valor.replace(' mi', '').replace(',', '.')) * 1000000)
        else:
            return int(valor.replace(',', '.'))
    df = pd.read_csv(file_path)
    if 'views' in df.columns:
        df['subscribers'] = df['subscribers'].apply(converter_valor)
        df.to_csv(file_path, index=False)
        return df[['subscribers']]
    else:
        return "A coluna 'subscribers' não existe no arquivo CSV."
    
def tratar_views(caminho_arquivo):
    try:
        df = pd.read_csv(caminho_arquivo)
        df['views'] = df['views'].str.replace('.', '', regex=False).astype('Int64')
        print(df)
        df.to_csv(caminho_arquivo, index=False)
        
        print(f"Arquivo {caminho_arquivo} atualizado com sucesso.")
    except Exception as e:
        print(f"Ocorreu um erro ao tentar ler o arquivo CSV: {e}")
        
'''def tratar_lifetime_int(file_path):
    df = pd.read_csv(file_path)
    df['views'] = df['views'].str.replace('.', '', regex=False).astype('int64')
    df['videos'] = df['videos'].astype(str).str.replace('.0', '').str.replace('.', '').str.replace('.', '', regex=False).astype(int)
    return df'''

def tratar_videos(file_path):
    df = pd.read_csv(file_path)
    df['videos'] = df['videos'].astype(str).str.replace('.', '').astype(int)
    df.to_csv(file_path, index=False)
    
def about_execute():
    with open('canais.txt', 'r') as file:
        channel_urls = file.readlines()
    
    results = []

    for channel_url in channel_urls:
        channel_url = channel_url.strip()
        print(f"Processando canal: {channel_url}")
        
        try:
            response = get_about(channel_url + "/about")
            results.append(response)
            print(f"-> {response}") 
            
        except Exception as e:
            print(f"Erro ao processar o canal {channel_url}: {e}")

    df = pd.DataFrame(results)
    df.to_csv('Lifetime.csv', index=False)
    print("Dados salvos em Lifetime.csv")

# Recentes e Top Uploads

def get_youtube_videos(channel_url):
    driver = init_webdriver()
    try:
        driver.get(channel_url)
        time.sleep(2)
        
        try:
            videos_titles = driver.find_elements(By.XPATH, '//*[@id="video-title"]')
            videos_views = driver.find_elements(By.XPATH, '//*[@id="metadata-line"]/span[1]')
            videos_urls = driver.find_elements(By.XPATH, '//*[@id="video-title-link"]')
            
            data = []
            for i in range(min(5, len(videos_titles))):
                data.append({
                    "name_video": videos_titles[i].text,
                    "views": videos_views[i].text,
                    "url": videos_urls[i].get_attribute('href')
                })
        except Exception as e:
            print(f"Erro ao obter vídeos do canal: {e}")
            data = []
        
        return data
    finally:
        driver.quit()

def get_comments(video_url):
    driver = init_webdriver()
    
    try:
        driver.get(video_url)
        time.sleep(5) 
        
        actions = ActionChains(driver)
        actions.scroll_by_amount(0, 10000).perform()
        time.sleep(10)
        
        try:
            comments = driver.find_element(By.XPATH, '/html/body/ytd-app/div[1]/ytd-page-manager/ytd-watch-flexy/div[5]/div[1]/div/div[2]/ytd-comments/ytd-item-section-renderer/div[1]/ytd-comments-header-renderer/div[1]/div[1]/h2/yt-formatted-string/span[1]').text
        except Exception as e:
            print(f"Erro ao obter comentários: {e}")
            comments = "N/A"

        return {
            "comments": comments
        }
        
    finally:
        driver.quit()

def get_youtube_top_videos(channel_url):
    driver = init_webdriver()
    try:
        driver.get(channel_url)
        time.sleep(5)
        
        try:
            hot_videos_element = driver.find_element(By.XPATH, '//*[@id="chips"]/yt-chip-cloud-chip-renderer[2]')
            actions = ActionChains(driver)
            actions.click(hot_videos_element).perform()
            time.sleep(5)
            
            videos_titles = driver.find_elements(By.XPATH, '//*[@id="video-title"]')
            videos_views = driver.find_elements(By.XPATH, '//*[@id="metadata-line"]/span[1]')
            videos_urls = driver.find_elements(By.XPATH, '//*[@id="video-title-link"]')
            
            data = []
            for i in range(min(5, len(videos_titles))):
                data.append({
                    "name_video": videos_titles[i].text,
                    "views": videos_views[i].text,
                    "url": videos_urls[i].get_attribute('href')
                })
        except Exception as e:
            print(f"Erro ao obter top vídeos do canal: {e}")
            data = []
        
        return data
    finally:
        driver.quit()

def recent_and_top_uploads():
    with open('canais.txt', 'r') as file:
        channel_urls = file.readlines()
    
    recent_videos_results = []
    top_videos_results = []

    for channel_url in channel_urls:
        channel_url = channel_url.strip()
        
        print(f"Processando canal: {channel_url}")
        
        try:
            # Coleta dos vídeos mais recentes
            recent_videos_info = get_youtube_videos(channel_url + "/videos")
            print(recent_videos_info)

            for video in recent_videos_info:
                print(f"Processing URL: {video['url']}")
                result = get_youtube_likes(video['url'])
                video['likes'] = result['likes']
                comments = get_comments(video['url'])
                video['comments'] = comments['comments']
                recent_videos_results.append(video)
                print(f"-> {result}")
                print(f"-> {comments}")

            # Coleta dos top vídeos
            top_videos_info = get_youtube_top_videos(channel_url + "/videos")
            print(top_videos_info)

            for video in top_videos_info:
                print(f"Processing URL: {video['url']}")
                result = get_youtube_likes(video['url'])
                video['likes'] = result['likes']
                comments = get_comments(video['url'])
                video['comments'] = comments['comments']
                top_videos_results.append(video)
                print(f"-> {result}")
                print(f"-> {comments}")

        except Exception as e:
            print(f"Erro ao processar o canal {channel_url}: {e}")
    
    # Salvando os dados em arquivos CSV
    df_recent_videos = pd.DataFrame(recent_videos_results)
    print(df_recent_videos)
    df_recent_videos.to_csv('RecentUploads.csv', index=False)

    df_top_videos = pd.DataFrame(top_videos_results)
    print(df_top_videos)
    df_top_videos.to_csv('TopUploads.csv', index=False)



def get_youtube_likes(video_url):
    driver = init_webdriver()
    try:
        driver.get(video_url)
        time.sleep(2)
        
        try:
            likes = driver.find_element(By.XPATH, '/html/body/ytd-app/div[1]/ytd-page-manager/ytd-watch-flexy/div[5]/div[1]/div/div[2]/ytd-watch-metadata/div/div[2]/div[2]/div/div/ytd-menu-renderer/div[1]/segmented-like-dislike-button-view-model/yt-smartimation/div/div/like-button-view-model/toggle-button-view-model/button-view-model/button/div[2]').text
        except:
            likes = "No likes available"
        
        return {"likes": likes}
        
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        return {"likes": None}
        
    finally:
        driver.quit()


# Inicializar função para coletar os dados do SocialBlade

def get_socialblade_data(channel_url):
    options = Options()
    options.add_argument("--headless")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    
    try:
        driver.get(channel_url)
        time.sleep(5)
        
        try:
            xpaths = [
                '//*[@id="socialblade-user-content"]/div[21]/div[2]/span',
                '//*[@id="socialblade-user-content"]/div[21]/div[3]/span',
                '//*[@id="socialblade-user-content"]/div[22]/div[2]/span',
                '//*[@id="socialblade-user-content"]/div[22]/div[3]/span',
                '//*[@id="socialblade-user-content"]/div[23]/div[2]/span',
                '//*[@id="socialblade-user-content"]/div[23]/div[3]/span'
            ]

            labels = ['daily_subs', 'daily_views', 'weekly_subs', 'weekly_views', 'monthly_subs', 'monthly_views']
            elements = {label: driver.find_element(By.XPATH, xpath).text for label, xpath in zip(labels, xpaths)}
        except Exception as e:
            print(f"Erro ao obter dados do SocialBlade: {e}")
            elements = {label: "N/A" for label in labels}

        return elements

    finally:
        driver.quit()
        
def last_30_days():
    with open('canais.txt', 'r') as file:
        canais = file.readlines()

    channel_data = []
    
    for canal in canais:
        canal = canal.strip()
        if '@' in canal:
            channel_name = canal.split('@')[-1]
            channel_url = f"https://socialblade.com/youtube/c/@{channel_name}"
            print(channel_url)
            data = get_socialblade_data(channel_url)
            data['channel'] = channel_name
            channel_data.append(data)
    
    with open('Last30Days.csv', 'w', newline='') as csvfile:
        fieldnames = ['channel', 'daily_subs', 'daily_views', 'weekly_subs', 'weekly_views', 'monthly_subs', 'monthly_views']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for data in channel_data:
            writer.writerow(data)


# Inicializar funções principais

def about():
    about_execute()
    '''
    tratar_data_lifetime('Lifetime.csv')
    lifetime_remove()
    tratar_subs('Lifetime.csv')
    tratar_lifetime_int('Lifetime.csv')
    tratar_views('Lifetime.csv')
    tratar_videos('Lifetime.csv')
    formatacao_lifetime('Lifetime.csv')
    '''
    print("Lifetime finalizado")

# Inicializar main
def main():
    about()
    recent_and_top_uploads()
    last_30_days()

# Executar main
if __name__ == "__main__":
    main()
