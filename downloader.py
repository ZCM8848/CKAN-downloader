from pathlib import Path, PurePath
import logging
import time
import json
import hashlib
import tarfile
import shutil
from tqdm import tqdm
import requests


class CKANDownloader:
    """CKAN模组下载器"""
    
    META_URL = "https://github.com/KSP-CKAN/CKAN-meta/archive/master.tar.gz"
    CHUNK_SIZE = 8192  # 8KB chunks for download
    
    def __init__(self,
                 cache_path: Path,
                 metadata_path: Path,
                 max_retry: int = 3):
        self.cache_path = cache_path
        self.metadata_path = metadata_path
        self.max_retry = max_retry
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.success = list()
        self.fail = list()
        self.to_download = list()
        
        # 确保目录存在
        self.cache_path.mkdir(parents=True, exist_ok=True)
        self.metadata_path.mkdir(parents=True, exist_ok=True)
    
    @staticmethod
    def _cache_prefix(download_url: str) -> str:
        """
        计算 CKAN 缓存文件名的 8 位十六进制前缀
        
        Args:
            download_url: 模组下载链接（注意去除尾随空格）
        
        Returns:
            8 位大写十六进制字符串（SHA1 前 8 位）
        """
        url = download_url.strip()
        sha1_hash = hashlib.sha1(url.encode('utf-8')).hexdigest()
        return sha1_hash[:8].upper()
    
    def _build_cache_filename(self, download_url: str, identifier: str, version: str) -> str:
        """构建缓存文件名: PREFIX-Identifier-Version.zip"""
        prefix = self._cache_prefix(download_url)
        # 清理版本号中的特殊字符
        safe_version = str(version).replace(':', '_')
        return f"{prefix}-{identifier}-{safe_version}.zip"
    
    def _download_file(self, url: str, path: Path, desc: str = "") -> Path | None:
        """
        下载单个文件，带重试机制和进度条
        
        Args:
            url: 下载URL
            path: 保存路径
            desc: 进度条描述
            
        Returns:
            下载成功的文件路径，失败返回None
        """
        for attempt in range(self.max_retry):
            try:
                response = requests.get(url, stream=True, timeout=30)
                response.raise_for_status()
                
                # 获取文件大小（如果可用）
                total_size = int(response.headers.get('content-length', 0))
                
                # 使用tqdm显示进度条
                progress_args = {
                    'total': total_size,
                    'unit': 'B',
                    'unit_scale': True,
                    'unit_divisor': 1024,
                    'desc': desc or "Downloading",
                    'leave': False,
                    'ncols': 80
                } if total_size > 0 else {
                    'total': None,
                    'unit': 'B',
                    'unit_scale': True,
                    'desc': desc or "Downloading",
                    'leave': False,
                    'ncols': 80
                }
                
                with open(path, 'wb') as f:
                    with tqdm(**progress_args) as pbar:
                        for chunk in response.iter_content(chunk_size=self.CHUNK_SIZE):
                            if chunk:
                                f.write(chunk)
                                if total_size > 0:
                                    pbar.update(len(chunk))
                
                print()  # 确保后续输出在新行
                return path
                
            except (requests.RequestException, IOError) as e:
                print()  # 确保错误信息在新行显示
                if attempt < self.max_retry - 1:
                    self.logger.warning(f"failed to download from {url} (attempt {attempt + 1}/{self.max_retry}): {e}")
                    time.sleep(1)
                else:
                    self.logger.error(f"cannot download from {url}, skipped: {e}")
                    # 清理部分下载的文件
                    if path.exists():
                        path.unlink()
                    return None
        return None
    
    def download_metadata(self) -> Path | None:
        """下载并解压CKAN元数据"""
        tar_path = self.metadata_path / "master.tar.gz"
        extract_path = self.metadata_path / "CKAN-meta-master"
        
        # 删除旧的tar.gz文件
        if tar_path.exists():
            self.logger.info(f"removing old archive: {tar_path}")
            tar_path.unlink()
        
        # 删除旧的解压目录
        if extract_path.exists():
            self.logger.info(f"removing old metadata directory: {extract_path}")
            shutil.rmtree(extract_path)
        
        # 下载新的元数据
        self.logger.info(f"downloading metadata from {self.META_URL}")
        downloaded = self._download_file(self.META_URL, tar_path, "Metadata")
        if not downloaded:
            self.logger.error("failed to download metadata archive")
            return None
        
        # 解压
        self.logger.info(f"extracting metadata to {self.metadata_path}")
        try:
            with tarfile.open(downloaded, "r:gz") as tar:
                tar.extractall(self.metadata_path)
            self.logger.info("metadata extraction complete")
            return extract_path
        except Exception as e:
            self.logger.error(f"failed to extract metadata: {e}")
            return None
    
    @staticmethod
    def _format_size(size_bytes: int) -> str:
        """将字节大小格式化为人类可读格式"""
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.2f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.2f} MB"
        else:
            return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"
    
    def _get_download_urls(self, download_field) -> list[str]:
        """
        获取下载URL列表
        download字段可能是字符串或字符串列表
        """
        if isinstance(download_field, str):
            return [download_field]
        elif isinstance(download_field, list):
            return [url for url in download_field if isinstance(url, str)]
        return []
    
    def scan_ckan_files(self, meta_dir: Path) -> list[dict]:
        """扫描所有.ckan和.kerbalstuff文件，返回下载任务列表"""
        ckan_files = list(meta_dir.rglob("*.ckan")) + list(meta_dir.rglob("*.kerbalstuff"))
        self.logger.info(f"found {len(ckan_files)} metadata files")
        
        tasks = []
        for ckan_file in ckan_files:
            try:
                with open(ckan_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                download_field = data.get("download")
                identifier = data.get("identifier")
                version = data.get("version")
                
                if not download_field or not identifier or not version:
                    self.logger.warning(f"skipping {ckan_file}: missing required fields")
                    continue
                
                # 处理download可能是字符串或列表的情况
                download_urls = self._get_download_urls(download_field)
                if not download_urls:
                    self.logger.warning(f"skipping {ckan_file}: invalid download field")
                    continue
                
                # 使用第一个URL作为主URL（CKAN通常如此）
                primary_url = download_urls[0]
                cache_filename = self._build_cache_filename(primary_url, identifier, version)
                cache_file_path = self.cache_path / cache_filename
                
                # 如果文件已存在，跳过
                if cache_file_path.exists():
                    continue
                
                # 获取文件大小信息
                download_size = data.get("download_size", 0)
                install_size = data.get("install_size", 0)
                
                tasks.append({
                    "url": primary_url,
                    "urls": download_urls,  # 保存所有备用URL
                    "identifier": identifier,
                    "version": version,
                    "filename": cache_filename,
                    "path": cache_file_path,
                    "ckan_file": str(ckan_file.relative_to(meta_dir)),
                    "download_size": download_size,
                    "install_size": install_size
                })
            except (json.JSONDecodeError, Exception) as e:
                self.logger.warning(f"failed to parse {ckan_file}: {e}")
                continue
        
        return tasks
    
    def estimate_total_size(self, tasks: list[dict] | None = None) -> dict:
        """
        估算总下载大小和安装后大小
        
        Args:
            tasks: 下载任务列表，如果为None则自动扫描
            
        Returns:
            包含下载大小和安装大小的字典
        """
        if tasks is None:
            meta_dir = self.metadata_path / "CKAN-meta-master"
            if not meta_dir.exists():
                self.logger.error("metadata not found, please run download_metadata() first")
                return {"download_size": 0, "install_size": 0, "count": 0}
            tasks = self.scan_ckan_files(meta_dir)
        
        total_download = sum(task.get("download_size", 0) for task in tasks)
        total_install = sum(task.get("install_size", 0) for task in tasks)
        unknown_count = sum(1 for task in tasks if task.get("download_size", 0) == 0)
        
        result = {
            "download_size": total_download,
            "install_size": total_install,
            "count": len(tasks),
            "unknown_count": unknown_count
        }
        
        return result
    
    def print_size_estimate(self, tasks: list[dict] | None = None) -> dict:
        """
        打印空间估算信息
        
        Args:
            tasks: 下载任务列表，如果为None则自动扫描
            
        Returns:
            大小估算结果字典
        """
        result = self.estimate_total_size(tasks)
        
        print("\n=== Space Estimate ===")
        print(f"  Total files: {result['count']}")
        print(f"  Download size: {self._format_size(result['download_size'])}")
        print(f"  Install size: {self._format_size(result['install_size'])}")
        if result['unknown_count'] > 0:
            print(f"  Note: {result['unknown_count']} files have unknown size")
        print("=" * 30)
        
        return result
    
    def list_pending_downloads(self, tasks: list[dict] | None = None) -> list[dict]:
        """
        列出所有待下载的文件
        
        如果未提供tasks，会自动扫描元数据
        
        Returns:
            待下载任务列表
        """
        if tasks is None:
            meta_dir = self.metadata_path / "CKAN-meta-master"
            if not meta_dir.exists():
                self.logger.error("metadata not found, please run download_metadata() first")
                return []
            tasks = self.scan_ckan_files(meta_dir)
        
        if not tasks:
            print("\nNo pending downloads found.")
            return []
        
        # 计算总大小
        total_download = sum(task.get("download_size", 0) for task in tasks)
        
        print(f"\n=== Pending Downloads ({len(tasks)} files) ===")
        print(f"  Total download size: {self._format_size(total_download)}")
        print()
        
        for task in tasks:
            size_str = self._format_size(task.get("download_size", 0)) if task.get("download_size") else "unknown"
            print(f"  {task['filename']} ({size_str})")
            print(f"    URL: {task['url']}")
            print(f"    Save to: {task['path']}")
        print("=" * 50)
        
        return tasks
    
    def download_mods(self, tasks: list[dict] | None = None) -> tuple[int, int]:
        """
        下载所有模组
        
        Args:
            tasks: 下载任务列表，如果为None则自动扫描
            
        Returns:
            (成功数量, 失败数量)
        """
        if tasks is None:
            meta_dir = self.metadata_path / "CKAN-meta-master"
            if not meta_dir.exists():
                self.logger.error("metadata not found, please run download_metadata() first")
                return 0, 0
            tasks = self.scan_ckan_files(meta_dir)
        
        if not tasks:
            self.logger.info("no mods to download")
            return 0, 0
        
        self.logger.info(f"starting download of {len(tasks)} mods")
        success_count = 0
        fail_count = 0
        
        # 总体进度条
        with tqdm(total=len(tasks), desc="Overall Progress", unit="mod") as overall_pbar:
            for task in tasks:
                desc = f"{task['identifier']}-{task['version']}"
                result = self._download_file(task['url'], task['path'], desc)
                
                if result:
                    self.success.append(task)
                    success_count += 1
                    self.logger.info(f"downloaded: {task['filename']}")
                else:
                    self.fail.append(task)
                    fail_count += 1
                    self.logger.error(f"failed: {task['filename']}")
                
                overall_pbar.update(1)
                overall_pbar.set_postfix(success=success_count, fail=fail_count)
        
        self.logger.info(f"download complete: {success_count} succeeded, {fail_count} failed")
        return success_count, fail_count
    
    def run(self) -> tuple[int, int]:
        """
        运行完整的下载流程
        
        Returns:
            (成功数量, 失败数量)
        """
        # 1. 下载元数据
        meta_dir = self.download_metadata()
        if not meta_dir:
            return 0, 0
        
        # 2. 扫描.ckan文件
        tasks = self.scan_ckan_files(meta_dir)
        self.to_download = tasks
        
        # 3. 显示空间估算
        self.print_size_estimate(tasks)
        
        # 4. 显示待下载列表
        self.list_pending_downloads(tasks)
        
        # 5. 下载模组
        return self.download_mods(tasks)


if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 示例用法
    downloader = CKANDownloader(
        cache_path=Path("./cache"),
        metadata_path=Path("./metadata"),
        max_retry=3
    )
    
    success, fail = downloader.run()
    print(f"\nSummary: {success} succeeded, {fail} failed")
