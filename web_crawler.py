#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import re
import sys
import time
from collections import deque
from urllib.parse import urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup
from tqdm import tqdm


class WebCrawler:
    def __init__(self, start_url, max_connections=20000, timeout=20, verify_ssl=False):
        self.start_url = start_url
        self.base_domain = urlparse(start_url).netloc
        self.visited_urls = set()  # 已访问的URL集合
        self.urls_to_visit = deque([start_url])  # 待访问的URL队列
        self.semaphore = asyncio.Semaphore(max_connections)  # 限制并发连接数
        self.timeout = timeout
        self.verify_ssl = verify_ssl  # 是否验证SSL证书
        self.session = None
        self.progress_bar = None
        
    async def init_session(self):
        """初始化HTTP会话"""
        # 创建连接器时设置ssl参数
        connector = aiohttp.TCPConnector(ssl=self.verify_ssl)
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.timeout),
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            },
            connector=connector
        )
        
    async def close_session(self):
        """关闭HTTP会话"""
        if self.session:
            await self.session.close()
        
    def normalize_url(self, url, base_url):
        """规范化URL"""
        # 处理相对路径
        full_url = urljoin(base_url, url)
        # 移除URL片段
        parsed = urlparse(full_url)
        normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        if parsed.query:
            normalized += f"?{parsed.query}"
        return normalized
    
    def is_valid_url(self, url):
        """检查URL是否有效且属于同一域名"""
        parsed = urlparse(url)
        # 确保URL是http或https
        if parsed.scheme not in ('http', 'https'):
            return False
        # 确保URL属于同一域名
        if parsed.netloc != self.base_domain:
            return False
        # 过滤媒体文件、但保留js和css
        extensions = ['.jpg', '.jpeg', '.png', '.gif', '.pdf', '.zip']
        if any(parsed.path.lower().endswith(ext) for ext in extensions):
            return False
        return True
        
    async def fetch_url(self, url):
        """异步获取URL内容"""
        async with self.semaphore:
            try:
                async with self.session.get(url, allow_redirects=True) as response:
                    if response.status == 200 and 'text/html' in response.headers.get('Content-Type', ''):
                        return await response.text()
                    return None
            except aiohttp.ClientSSLError as e:
                print(f"SSL证书错误 {url}: {e}")
                return None
            except aiohttp.ClientConnectorSSLError as e:
                print(f"SSL连接错误 {url}: {e}")
                return None
            except Exception as e:
                # print(f"Error fetching {url}: {e}")
                return None
                
    async def parse_links(self, html, base_url):
        """解析HTML页面中的链接"""
        if not html:
            return []
            
        soup = BeautifulSoup(html, 'html.parser')
        links = []
        
        # 查找所有a标签的href属性
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href'].strip()
            # 忽略空链接和JavaScript链接
            if not href or href.startswith('javascript:') or href == '#':
                continue
            
            # 规范化URL
            normalized_url = self.normalize_url(href, base_url)
            if self.is_valid_url(normalized_url) and normalized_url not in self.visited_urls:
                links.append(normalized_url)
        
        # 查找所有带src属性的标签（如img, script, iframe等）
        for tag in soup.find_all(src=True):
            src = tag['src'].strip()
            if not src or src.startswith('data:'):  # 忽略空链接和data URI
                continue
                
            # 规范化URL
            normalized_url = self.normalize_url(src, base_url)
            if self.is_valid_url(normalized_url) and normalized_url not in self.visited_urls:
                links.append(normalized_url)
                
        return links
        
    async def process_url(self, url):
        """处理单个URL"""
        if url in self.visited_urls:
            return []
            
        self.visited_urls.add(url)
        html = await self.fetch_url(url)
        new_links = await self.parse_links(html, url)
        
        # 更新进度条
        if self.progress_bar is not None:
            self.progress_bar.update(1)
            
        return new_links
        
    async def crawl(self):
        """开始爬取网站"""
        await self.init_session()
        
        # 初始化进度条时指定total参数
        self.progress_bar = tqdm(desc="爬取进度", unit="页", total=None)
        pending_tasks = set()
        
        # 添加第一个URL任务
        task = asyncio.create_task(self.process_url(self.urls_to_visit.popleft()))
        pending_tasks.add(task)
        
        try:
            while pending_tasks:
                # 等待任意一个任务完成
                done, pending_tasks = await asyncio.wait(
                    pending_tasks, 
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                for task in done:
                    try:
                        new_links = task.result()
                        # 将新链接添加到队列
                        for link in new_links:
                            if link not in self.visited_urls and link not in self.urls_to_visit:
                                self.urls_to_visit.append(link)
                                
                        # 为待访问的URL创建新任务
                        while self.urls_to_visit and len(pending_tasks) < 50:
                            new_url = self.urls_to_visit.popleft()
                            new_task = asyncio.create_task(self.process_url(new_url))
                            pending_tasks.add(new_task)
                    except Exception as e:
                        print(f"处理任务时出错: {e}")
        finally:
            # 关闭进度条和会话
            if self.progress_bar is not None:
                self.progress_bar.close()
            await self.close_session()
        
    def save_results(self, filename):
        """将结果保存到文件"""
        with open(filename, 'w', encoding='utf-8') as f:
            for url in sorted(self.visited_urls):
                f.write(f"{url}\n")
                
    async def run(self):
        """运行爬虫"""
        start_time = time.time()
        await self.crawl()
        elapsed_time = time.time() - start_time
        
        print(f"\n爬取完成！")
        print(f"共爬取 {len(self.visited_urls)} 个URL")
        print(f"用时: {elapsed_time:.2f} 秒")
        
        output_file = f"{self.base_domain}_links.txt"
        self.save_results(output_file)
        print(f"结果已保存到 {output_file}")
        
        
async def main():
    """主函数"""
    if len(sys.argv) < 2:
        print("用法: python web_crawler.py <起始URL> [verify_ssl]")
        print("例如: python web_crawler.py https://example.com")
        print("或: python web_crawler.py https://example.com true")
        return
        
    start_url = sys.argv[1]
    verify_ssl = sys.argv[2].lower() == 'true' if len(sys.argv) > 2 else False
    crawler = WebCrawler(start_url, verify_ssl=verify_ssl)
    await crawler.run()
    

if __name__ == "__main__":
    asyncio.run(main()) 