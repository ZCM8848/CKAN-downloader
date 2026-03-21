import wget
from tqdm import tqdm
import requests
import hashlib
from pathlib import Path

url = "https://github.com/KSP-CKAN/CKAN-meta/archive/master.tar.gz"
print("downloading metadata")
name = wget.download(url, ".\\metadata")
print()
print(Path(name))

def ckan_cache_prefix(download_url: str) -> str:
    """
    计算 CKAN 缓存文件名的 8 位十六进制前缀
    
    Args:
        download_url: 模组下载链接（注意去除尾随空格）
    
    Returns:
        8 位大写十六进制字符串（SHA1 前 8 位）
    """
    # 去除首尾空格（JSON 中有时会带尾随空格）
    url = download_url.strip()
    
    # 计算 SHA1 哈希，取前 8 位，转大写
    sha1_hash = hashlib.sha1(url.encode('utf-8')).hexdigest()
    return sha1_hash[:8].upper()