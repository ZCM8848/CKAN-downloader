import hashlib
import json
import os
import shutil
import zipfile

import pyautogui
import requests
from tqdm import tqdm


def generate_prefix(file):
    # 打开文件，以二进制模式读取
    with open(file, "rb") as f:
        # 创建一个SHA1对象
        sha1 = hashlib.sha1()
        # 循环读取文件内容，每次读取1024字节
        while True:
            # 读取一块数据
            data = f.read(1024)
            # 如果数据为空，表示文件读取完毕，跳出循环
            if not data:
                break
            # 将数据更新到SHA1对象中
            sha1.update(data)
        # 返回SHA1对象的16进制表示，即SHA1哈希值
        return sha1.hexdigest()[0:8].upper()

def download(url: str, file_name: str):
        '''
        根据文件直链和文件名下载文件

        Parameters
        ----------
        url: 文件直链
        file_name : 文件名（文件路径）

        '''
        # 文件下载直链
        # 请求头
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36 QIHU 360SE'
        }

        # 发起 head 请求，即只会获取响应头部信息
        head = requests.head(url, headers=headers, timeout=200)
        # 文件大小，以 B 为单位
        file_size = head.headers.get('Content-Length')
        if file_size is not None:
            file_size = int(file_size)
        response = requests.get(url, headers=headers, stream=True)
        downloaded = f'{os.getcwd()}\\downloaded\\'
        temp_file_name = downloaded + 'temp.zip'
        while ' ' in temp_file_name:
            file_name.replace(' ', '')
        with open(temp_file_name, 'wb') as file, tqdm(
            desc=f'    downloading:{file_name}',
            total=file_size,
            unit='B',
            unit_scale=True,
            unit_divisor=1024,
        ) as bar:
            for data in response.iter_content(chunk_size=1024):
                size = file.write(data)
                bar.update(size)
        new_name = downloaded + generate_prefix(temp_file_name) + '-' + file_name
        os.rename(temp_file_name, new_name)
        print(f'    completed')
        return new_name

def rename(files):
    for file in files:
        os.rename(file, generate_prefix(file) + file)
    return None


def refresh_source():
    print('refreshing source')
    url = "https://github.com/KSP-CKAN/CKAN-meta/archive/refs/heads/master.zip"
    name = download(url, 'source.zip')
    meta = os.path.join(os.getcwd(), 'CKAN-meta-master')
    # check if the meta directory exists before deleting it
    if os.path.exists(meta):
        shutil.rmtree(meta)
    # use with statement to open and close the zip file
    with zipfile.ZipFile(name) as zip:
        zip.extractall(path=meta)
    # check if the name file exists before deleting it
    if os.path.exists(name):
        os.remove(name)
    return None

def get_task(dir_path):
    todo = []
    # 使用os模块的walk方法，遍历目录下的所有子目录和文件
    for root, dirs, files in os.walk(dir_path):
        if not dirs:
            for i in range(len(files)):
                todo.append(f'{root}//{files[i]}')
    return todo

def get_downloaded(dir_path):
    todo = []
    for root, dirs, files in os.walk(dir_path):
            if not dirs:
                for i in range(len(files)):
                    todo.append(f'{root}//{files[i]}')
    return todo

def parse(file_name):
    try:
        #print(f'parsing:{file_name}')
        # 使用open函数和read方法，将文件的内容读取到一个字符串变量中
        file_content = open(file_name, "r", encoding='utf-8').read()
        # 使用json模块的loads方法，将字符串变量转换为一个Python字典对象
        file_dict = json.loads(file_content)
        # 使用字典对象的get方法，根据键的名称获取对应的值，并打印出来
        download_value = file_dict.get("download")
        identifier_value = file_dict.get("identifier")
        size = file_dict.get("download_size")
        version = file_dict.get("version")
        #print(f"The value of 'download' is: {download_value}")
        #print(f"The value of 'identifier' is: {identifier_value}")
        return download_value, identifier_value, size, version
    except:
        return None, None, None

def check(to_download):
        l = []
        size_sum = 0
        for mod in to_download:
            try:
                a, b, c, d = parse(mod)
                if a and b and c:
                    print(f'checked: {mod}')
                    l.append(mod)
                    size_sum = size_sum + (c/1024/1024/1024)
            except:
                pass
        return l, size_sum

def ckan_cache(file_list):
    ckan = os.path.join(os.getcwd(), 'fake', 'ckan.exe')
    ckan_cache = os.path.join(os.getcwd(), 'fake', 'ckan_cache')
    try:
        os.system(f'{ckan} cache set {ckan_cache}')
        for mod in file_list:
            os.system(f'{ckan} import {mod}')
            pyautogui.press('n')
            pyautogui.press('enter')
    except:
        print('bad file!')
    return None

def ckan_install():
    ckan = os.path.join(os.getcwd(), 'fake', 'ckan.exe')
    os.system(f'{ckan} update')
    to_import = get_downloaded('downloaded')
    for i in range(len(to_import)):
        to_import[i] = os.path.join(os.getcwd(), to_import[i])
    print(to_import)
    ckan_cache(to_import)

def main():
    failed = []
    refresh_source()
    print("""To update the source, please visit "https://github.com/KSP-CKAN/CKAN-meta/archive/refs/heads/master.zip" """)
    to_download, size = check(get_task(f'{os.getcwd()}//CKAN-meta-master//'))
    print(f'The following mods are going to be downloaded:')
    for mod in to_download:
        print(f'    {mod}')
    os.system('cls')
    print(f'{len(to_download)} mods, {round(size,2)}GB in all')
    for mod in to_download:
        try:
            print(f'parsing {mod}')
            link, name, size, version = parse(mod)
            download(link, name + version + '.zip')
        except:
            failed.append(mod)
    ckan_install()
    print(f'The following {len(failed)} mods are NOT downloadeed:')
    for mod in failed:
        print(f'    {mod}')
    input('press ENTER to exit...')

main()