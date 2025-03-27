#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import re
import sys
import time
from urllib.parse import urljoin, urlparse

import aiohttp
import redis
from bs4 import BeautifulSoup
from tqdm import tqdm


class RedisCrawler:
    def __init__(self, start_url, max_connections=20000, timeout=10, 
                 redis_host='localhost', redis_port=6379, redis_db=0,
                 verify_ssl=False):
        """初始化Redis爬虫"""
        self.start_url = start_url
        self.base_domain = urlparse(start_url).netloc
        
        # Redis连接
        self.redis = redis.Redis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)
        
        # Redis键名
        self.visited_key = f"crawler:{self.base_domain}:visited"
        self.queue_key = f"crawler:{self.base_domain}:queue"
        
        # 并发控制
        self.semaphore = asyncio.Semaphore(max_connections)
        self.timeout = timeout
        self.verify_ssl = verify_ssl  # 是否验证SSL证书
        self.session = None
        self.progress_bar = None
        
        # 初始化Redis
        self._init_redis()
        
    def _init_redis(self):
        """初始化Redis数据结构"""
        # 如果队列为空，添加起始URL
        if self.redis.llen(self.queue_key) == 0:
            self.redis.lpush(self.queue_key, self.start_url)
        
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
            if self.is_valid_url(normalized_url):
                links.append(normalized_url)
                
        # 查找所有带src属性的标签（如img, script, iframe等）
        for tag in soup.find_all(src=True):
            src = tag['src'].strip()
            if not src or src.startswith('data:'):  # 忽略空链接和data URI
                continue
                
            # 规范化URL
            normalized_url = self.normalize_url(src, base_url)
            if self.is_valid_url(normalized_url):
                links.append(normalized_url)
                
        return links
        
    async def process_url(self, url):
        """处理单个URL，返回新发现的链接"""
        # 检查URL是否已处理
        if self.redis.sismember(self.visited_key, url):
            return []
        
        # 标记为已访问
        self.redis.sadd(self.visited_key, url)
        
        html = await self.fetch_url(url)
        new_links = await self.parse_links(html, url)
        
        # 添加新链接到队列
        for link in new_links:
            if not self.redis.sismember(self.visited_key, link):
                self.redis.lpush(self.queue_key, link)
                
        # 更新进度条
        if self.progress_bar is not None:
            self.progress_bar.update(1)
            
        return new_links
        
    async def crawl(self):
        """开始爬取网站"""
        await self.init_session()
        
        # 初始化进度条
        total_urls = self.redis.llen(self.queue_key)
        self.progress_bar = tqdm(desc="爬取进度", unit="页", total=total_urls)
        
        # 任务集合
        pending_tasks = set()
        max_concurrent_tasks = 100
        
        # 只要队列不为空或有任务正在处理，就继续循环
        while self.redis.llen(self.queue_key) > 0 or pending_tasks:
            
            # 当任务数少于最大并发数且队列不为空时，添加新任务
            while len(pending_tasks) < max_concurrent_tasks and self.redis.llen(self.queue_key) > 0:
                url = self.redis.rpop(self.queue_key)
                if url and not self.redis.sismember(self.visited_key, url):
                    task = asyncio.create_task(self.process_url(url))
                    pending_tasks.add(task)
                    # 为任务添加回调，以便从集合中移除
                    task.add_done_callback(lambda t: pending_tasks.discard(t))
            
            # 如果任务集合为空但队列不为空，等待一小段时间
            if not pending_tasks and self.redis.llen(self.queue_key) > 0:
                await asyncio.sleep(0.1)
                continue
                
            # 如果任务集合不为空，等待任务完成
            if pending_tasks:
                # 等待任意一个任务完成
                done, _ = await asyncio.wait(
                    pending_tasks, 
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                # 更新总数
                current_total = self.redis.llen(self.queue_key) + len(pending_tasks) + self.redis.scard(self.visited_key)
                if self.progress_bar.total != current_total:
                    self.progress_bar.total = current_total
                
        # 关闭资源
        if self.progress_bar is not None:
            self.progress_bar.close()
        await self.close_session()
        
    def export_results(self, filename):
        """将结果导出到文件"""
        with open(filename, 'w', encoding='utf-8') as f:
            for url in self.redis.smembers(self.visited_key):
                f.write(f"{url}\n")
                
    def count_results(self):
        """计算结果总数"""
        return self.redis.scard(self.visited_key)
        
    async def run(self):
        """运行爬虫"""
        start_time = time.time()
        await self.crawl()
        elapsed_time = time.time() - start_time
        
        total_urls = self.count_results()
        
        print(f"\n爬取完成！")
        print(f"共爬取 {total_urls} 个URL")
        print(f"用时: {elapsed_time:.2f} 秒")
        
        output_file = f"{self.base_domain}_links.txt"
        self.export_results(output_file)
        print(f"结果已保存到 {output_file}")
        

async def main():
    """主函数"""
    if len(sys.argv) < 2:
        print("用法: python web_crawler_redis.py <起始URL> [Redis主机 [Redis端口 [Redis数据库 [verify_ssl]]]]")
        print("例如: python web_crawler_redis.py https://example.com")
        print("或: python web_crawler_redis.py https://example.com localhost 6379 0 false")
        return
        
    start_url = sys.argv[1]
    redis_host = sys.argv[2] if len(sys.argv) > 2 else 'localhost'
    redis_port = int(sys.argv[3]) if len(sys.argv) > 3 else 6379
    redis_db = int(sys.argv[4]) if len(sys.argv) > 4 else 0
    verify_ssl = sys.argv[5].lower() == 'true' if len(sys.argv) > 5 else False
    
    crawler = RedisCrawler(
        start_url, 
        redis_host=redis_host,
        redis_port=redis_port,
        redis_db=redis_db,
        verify_ssl=verify_ssl
    )
    await crawler.run()
    

if __name__ == "__main__":
    asyncio.run(main()) 