#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import re
import sys
import time
from urllib.parse import urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup
from motor.motor_asyncio import AsyncIOMotorClient
from tqdm import tqdm


class MongoCrawler:
    def __init__(self, start_url, max_connections=1000, timeout=10, 
                 mongo_uri='mongodb://localhost:27017', db_name=None,
                 verify_ssl=False):
        """初始化MongoDB爬虫"""
        self.start_url = start_url
        self.base_domain = urlparse(start_url).netloc
        
        # 使用域名作为数据库名，如果未指定
        if db_name is None:
            db_name = f"crawler_{self.base_domain.replace('.', '_')}"
        self.db_name = db_name
        
        # MongoDB连接
        self.mongo_uri = mongo_uri
        self.mongo_client = None
        self.db = None
        self.urls_collection = None
        self.queue_collection = None
        
        # 并发控制
        self.semaphore = asyncio.Semaphore(max_connections)
        self.timeout = timeout
        self.verify_ssl = verify_ssl  # 是否验证SSL证书
        self.session = None
        self.progress_bar = None
        
    async def init_mongodb(self):
        """初始化MongoDB连接和集合"""
        self.mongo_client = AsyncIOMotorClient(self.mongo_uri)
        self.db = self.mongo_client[self.db_name]
        self.urls_collection = self.db.urls
        self.queue_collection = self.db.queue
        
        # 创建索引
        await self.urls_collection.create_index("url", unique=True)
        await self.queue_collection.create_index("url", unique=True)
        
        # 检查队列是否为空，如果为空则添加起始URL
        count = await self.queue_collection.count_documents({})
        if count == 0:
            await self.queue_collection.insert_one({"url": self.start_url, "added_at": time.time()})
            
    async def close_mongodb(self):
        """关闭MongoDB连接"""
        if self.mongo_client:
            self.mongo_client.close()
        
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
                        html = await response.text()
                        content_type = response.headers.get('Content-Type', '')
                        status = response.status
                        return html, status, content_type
                    return None, response.status, response.headers.get('Content-Type', '')
            except aiohttp.ClientSSLError as e:
                print(f"SSL证书错误 {url}: {e}")
                return None, 0, f"SSL证书错误: {str(e)}"
            except aiohttp.ClientConnectorSSLError as e:
                print(f"SSL连接错误 {url}: {e}")
                return None, 0, f"SSL连接错误: {str(e)}"
            except Exception as e:
                # print(f"Error fetching {url}: {e}")
                return None, 0, str(e)
                
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
                # 提取链接文本
                link_text = a_tag.get_text().strip()
                links.append((normalized_url, link_text))
                
        # 查找所有带src属性的标签（如img, script, iframe等）
        for tag in soup.find_all(src=True):
            src = tag['src'].strip()
            if not src or src.startswith('data:'):  # 忽略空链接和data URI
                continue
                
            # 规范化URL
            normalized_url = self.normalize_url(src, base_url)
            if self.is_valid_url(normalized_url):
                # 获取标签名作为描述
                tag_name = tag.name
                links.append((normalized_url, f"资源: {tag_name}"))
                
        return links
        
    async def process_url(self, url):
        """处理单个URL"""
        # 检查URL是否已处理
        if await self.urls_collection.find_one({"url": url}):
            return []
            
        html, status, content_type = await self.fetch_url(url)
        new_links = []
        
        if html:
            new_links = await self.parse_links(html, url)
            
            # 保存页面信息到数据库
            document = {
                "url": url,
                "status": status,
                "content_type": content_type,
                "crawled_at": time.time(),
                "title": self.extract_title(html) if html else "",
                "links_count": len(new_links)
            }
            await self.urls_collection.insert_one(document)
            
            # 将新链接添加到队列
            for link_url, link_text in new_links:
                # 检查链接是否已经在已访问集合或队列中
                if (not await self.urls_collection.find_one({"url": link_url}) and 
                    not await self.queue_collection.find_one({"url": link_url})):
                    # 添加到队列
                    await self.queue_collection.insert_one({
                        "url": link_url,
                        "source_url": url,
                        "link_text": link_text,
                        "added_at": time.time()
                    })
        else:
            # 保存错误信息
            document = {
                "url": url,
                "status": status,
                "content_type": content_type,
                "crawled_at": time.time(),
                "error": True
            }
            await self.urls_collection.insert_one(document)
            
        # 更新进度条
        if self.progress_bar is not None:
            self.progress_bar.update(1)
            
        return [link[0] for link in new_links]  # 返回URL列表
        
    def extract_title(self, html):
        """从HTML中提取标题"""
        soup = BeautifulSoup(html, 'html.parser')
        title_tag = soup.find('title')
        return title_tag.get_text().strip() if title_tag else ""
        
    async def get_next_url(self):
        """从队列中获取下一个URL"""
        # 获取并删除一个文档
        doc = await self.queue_collection.find_one_and_delete({})
        return doc["url"] if doc else None
        
    async def crawl(self):
        """开始爬取网站"""
        await self.init_mongodb()
        await self.init_session()
        
        # 初始化进度条
        total_urls = await self.queue_collection.count_documents({})
        self.progress_bar = tqdm(desc="爬取进度", unit="页", total=total_urls)
        
        # 任务集合
        pending_tasks = set()
        max_concurrent_tasks = 100
        
        # 只要队列不为空或有任务正在处理，就继续循环
        while (await self.queue_collection.count_documents({})) > 0 or pending_tasks:
            
            # 当任务数少于最大并发数且队列不为空时，添加新任务
            while len(pending_tasks) < max_concurrent_tasks and (await self.queue_collection.count_documents({})) > 0:
                url = await self.get_next_url()
                if url:
                    task = asyncio.create_task(self.process_url(url))
                    pending_tasks.add(task)
                    # 为任务添加回调，以便从集合中移除
                    task.add_done_callback(lambda t: pending_tasks.discard(t))
            
            # 如果任务集合为空但队列不为空，等待一小段时间
            if not pending_tasks and (await self.queue_collection.count_documents({})) > 0:
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
                current_total = (await self.queue_collection.count_documents({})) + len(pending_tasks) + (await self.urls_collection.count_documents({}))
                if self.progress_bar.total != current_total:
                    self.progress_bar.total = current_total
                
        # 关闭资源
        if self.progress_bar is not None:
            self.progress_bar.close()
        await self.close_session()
        await self.close_mongodb()
        
    async def export_results(self, filename):
        """将结果导出到文件"""
        await self.init_mongodb()
        
        with open(filename, 'w', encoding='utf-8') as f:
            async for doc in self.urls_collection.find({}, {"url": 1, "title": 1}).sort("url", 1):
                title = doc.get("title", "")
                f.write(f"{doc['url']} - {title}\n")
                
        await self.close_mongodb()
                
    async def count_results(self):
        """计算结果总数"""
        await self.init_mongodb()
        count = await self.urls_collection.count_documents({})
        await self.close_mongodb()
        return count
        
    async def run(self):
        """运行爬虫"""
        start_time = time.time()
        await self.crawl()
        elapsed_time = time.time() - start_time
        
        total_urls = await self.count_results()
        
        print(f"\n爬取完成！")
        print(f"共爬取 {total_urls} 个URL")
        print(f"用时: {elapsed_time:.2f} 秒")
        
        output_file = f"{self.base_domain}_links.txt"
        await self.export_results(output_file)
        print(f"结果已保存到 {output_file}")
        

async def main():
    """主函数"""
    if len(sys.argv) < 2:
        print("用法: python web_crawler_mongo.py <起始URL> [MongoDB URI [数据库名 [verify_ssl]]]")
        print("例如: python web_crawler_mongo.py https://example.com")
        print("或: python web_crawler_mongo.py https://example.com mongodb://localhost:27017 my_crawler_db false")
        return
        
    start_url = sys.argv[1]
    mongo_uri = sys.argv[2] if len(sys.argv) > 2 else 'mongodb://localhost:27017'
    db_name = sys.argv[3] if len(sys.argv) > 3 else None
    verify_ssl = sys.argv[4].lower() == 'true' if len(sys.argv) > 4 else False
    
    crawler = MongoCrawler(
        start_url, 
        mongo_uri=mongo_uri,
        db_name=db_name,
        verify_ssl=verify_ssl
    )
    await crawler.run()
    

if __name__ == "__main__":
    asyncio.run(main()) 