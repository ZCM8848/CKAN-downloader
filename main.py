from pathlib import Path
from downloader import CKANDownloader
import logging

logging.basicConfig(level=logging.INFO)

downloader = CKANDownloader(
    cache_path=Path("./cache"),
    metadata_path=Path("./metadata"),
    max_retry=3
)

# 方法1: 运行完整流程
success, fail = downloader.run()

# 方法2: 分步调用
# downloader.download_metadata()
# tasks = downloader.list_pending_downloads()  # 打印待下载列表
# success, fail = downloader.download_mods(tasks) 