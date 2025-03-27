#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import hashlib
import json
import os
import random
import re
import socket
import sys
import time
from urllib.parse import urljoin, urlparse

import aiohttp
import redis
from bs4 import BeautifulSoup
from motor.motor_asyncio import AsyncIOMotorClient
from tqdm import tqdm


class DistributedCrawler:
    """分布式爬虫，使用Redis作为共享队列和去重机制，MongoDB存储结果"""
    
    def __init__(self, start_url=None, worker_id=None, max_connections=100, timeout=10,
                 redis_host='localhost', redis_port=6379, redis_db=0,
                 mongo_uri='mongodb://localhost:27017', mongo_db=None,
                 verify_ssl=False):
        """初始化分布式爬虫"""
        # 基本配置
        self.start_url = start_url
        self.base_domain = urlparse(start_url).netloc if start_url else None
        self.worker_id = worker_id or f"{socket.gethostname()}_{os.getpid()}_{random.randint(1000, 9999)}"
        
        # Redis配置
        self.redis = redis.Redis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)
        
        # MongoDB配置
        if mongo_db is None and self.base_domain:
            mongo_db = f"crawler_{self.base_domain.replace('.', '_')}"
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_client = None
        self.db = None
        
        # Redis键名
        self.domain_key = None
        self.queue_key = None
        self.visited_key = None
        self.worker_key = None
        self.stats_key = None
        
        # 并发控制
        self.semaphore = asyncio.Semaphore(max_connections)
        self.timeout = timeout
        self.verify_ssl = verify_ssl  # 是否验证SSL证书
        self.session = None
        self.progress_bar = None
        self.crawl_running = False
        
        # 性能统计
        self.start_time = None
        self.processed_count = 0
        self.error_count = 0
        
    def set_domain(self, domain):
        """设置要爬取的域名，并初始化Redis键"""
        self.base_domain = domain
        self.domain_key = f"crawler:domain:{domain}"
        self.queue_key = f"crawler:queue:{domain}"
        self.visited_key = f"crawler:visited:{domain}"
        self.worker_key = f"crawler:workers:{domain}"
        self.stats_key = f"crawler:stats:{domain}"
        
        # 设置MongoDB数据库名
        if self.mongo_db is None:
            self.mongo_db = f"crawler_{domain.replace('.', '_')}"
        
    def init_redis(self):
        """初始化Redis数据结构"""
        # 将当前worker注册到工作节点集合
        self.redis.sadd(self.worker_key, self.worker_id)
        self.redis.hset(f"{self.worker_key}:info", self.worker_id, json.dumps({
            "started_at": time.time(),
            "host": socket.gethostname(),
            "pid": os.getpid()
        }))
        
        # 设置worker心跳
        self.redis.setex(f"{self.worker_key}:heartbeat:{self.worker_id}", 30, time.time())
        
        # 如果队列为空且域名键不存在（表示是首次启动），添加起始URL
        if self.start_url and self.redis.llen(self.queue_key) == 0 and not self.redis.exists(self.domain_key):
            self.redis.lpush(self.queue_key, self.start_url)
            self.redis.set(self.domain_key, time.time())
        
    async def init_mongo(self):
        """初始化MongoDB连接"""
        self.mongo_client = AsyncIOMotorClient(self.mongo_uri)
        self.db = self.mongo_client[self.mongo_db]
        
        # 创建必要的集合和索引
        await self.db.urls.create_index("url", unique=True)
        await self.db.stats.create_index("timestamp")
        
    def update_heartbeat(self):
        """更新worker心跳"""
        if self.worker_key:
            self.redis.setex(f"{self.worker_key}:heartbeat:{self.worker_id}", 30, time.time())
            
    async def init_session(self):
        """初始化HTTP会话"""
        # 创建连接器时设置ssl参数
        connector = aiohttp.TCPConnector(ssl=self.verify_ssl)
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.timeout),
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Cache-Control': 'max-age=0'
            },
            connector=connector
        )
    
    async def close_session(self):
        """关闭HTTP会话"""
        if self.session:
            await self.session.close()
    
    async def close_mongo(self):
        """关闭MongoDB连接"""
        if self.mongo_client:
            self.mongo_client.close()
    
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
        # 过滤媒体文件，但保留js和css
        extensions = ['.jpg', '.jpeg', '.png', '.gif', '.pdf', '.zip', '.xml', '.ico']
        if any(parsed.path.lower().endswith(ext) for ext in extensions):
            return False
        return True
    
    async def fetch_url(self, url):
        """异步获取URL内容"""
        async with self.semaphore:
            try:
                self.update_heartbeat()
                async with self.session.get(url, allow_redirects=True) as response:
                    if response.status == 200 and 'text/html' in response.headers.get('Content-Type', ''):
                        html = await response.text()
                        content_type = response.headers.get('Content-Type', '')
                        status = response.status
                        return html, status, content_type
                    return None, response.status, response.headers.get('Content-Type', '')
            except aiohttp.ClientSSLError as e:
                print(f"SSL证书错误 {url}: {e}")
                self.error_count += 1
                return None, 0, f"SSL证书错误: {str(e)}"
            except aiohttp.ClientConnectorSSLError as e:
                print(f"SSL连接错误 {url}: {e}")
                self.error_count += 1
                return None, 0, f"SSL连接错误: {str(e)}"
            except Exception as e:
                self.error_count += 1
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
    
    def extract_title(self, html):
        """从HTML中提取标题"""
        soup = BeautifulSoup(html, 'html.parser')
        title_tag = soup.find('title')
        return title_tag.get_text().strip() if title_tag else ""
    
    async def process_url(self, url):
        """处理单个URL"""
        # 检查URL是否已处理
        if self.redis.sismember(self.visited_key, url):
            return []
        
        # 标记为已访问，使用SETNX确保只有一个worker处理此URL
        if not self.redis.setnx(f"{self.visited_key}:processing:{url}", self.worker_id):
            return []  # 另一个worker正在处理此URL
        
        # 获取页面内容
        html, status, content_type = await self.fetch_url(url)
        new_links = []
        
        try:
            # 添加到已访问集合
            self.redis.sadd(self.visited_key, url)
            
            # 将URL标记为已处理
            url_hash = hashlib.md5(url.encode()).hexdigest()
            
            if html:
                # 解析链接
                new_links = await self.parse_links(html, url)
                
                # 保存到MongoDB
                document = {
                    "url": url,
                    "url_hash": url_hash,
                    "status": status,
                    "content_type": content_type,
                    "crawled_at": time.time(),
                    "crawled_by": self.worker_id,
                    "title": self.extract_title(html),
                    "links_count": len(new_links)
                }
                
                # 将新链接添加到队列
                for link_url, link_text in new_links:
                    # 如果链接不在已访问集合中，添加到队列
                    if not self.redis.sismember(self.visited_key, link_url):
                        self.redis.lpush(self.queue_key, link_url)
                        
                await self.db.urls.update_one(
                    {"url_hash": url_hash},
                    {"$set": document},
                    upsert=True
                )
            else:
                # 保存错误信息
                document = {
                    "url": url,
                    "url_hash": url_hash,
                    "status": status,
                    "content_type": content_type,
                    "crawled_at": time.time(),
                    "crawled_by": self.worker_id,
                    "error": True
                }
                await self.db.urls.update_one(
                    {"url_hash": url_hash},
                    {"$set": document},
                    upsert=True
                )
        finally:
            # 删除处理标记
            self.redis.delete(f"{self.visited_key}:processing:{url}")
            
            # 更新进度条
            self.processed_count += 1
            if self.progress_bar is not None:
                self.progress_bar.update(1)
                
            # 更新统计信息
            self.redis.hincrby(self.stats_key, f"{self.worker_id}:processed", 1)
            if not html:
                self.redis.hincrby(self.stats_key, f"{self.worker_id}:errors", 1)
            
        return [link[0] for link in new_links]  # 返回URL列表
    
    async def get_next_url(self):
        """从队列中获取下一个URL"""
        # 使用BRPOP阻塞等待，但设置一个超时
        result = self.redis.brpop(self.queue_key, timeout=1)
        if result:
            _, url = result
            return url
        return None
    
    async def update_stats(self):
        """定期更新统计信息"""
        while self.crawl_running:
            self.update_heartbeat()
            
            # 计算队列长度、已处理URL数等统计信息
            queue_length = self.redis.llen(self.queue_key)
            visited_count = self.redis.scard(self.visited_key)
            
            # 更新到Redis
            stats = {
                "queue_length": queue_length,
                "visited_count": visited_count,
                "worker_count": self.redis.scard(self.worker_key),
                "last_updated": time.time()
            }
            self.redis.hmset(f"{self.stats_key}:global", stats)
            
            # 更新到MongoDB
            if self.db:
                await self.db.stats.insert_one({
                    "timestamp": time.time(),
                    "worker_id": self.worker_id,
                    "queue_length": queue_length,
                    "visited_count": visited_count,
                    "processed_count": self.processed_count,
                    "error_count": self.error_count
                })
                
            await asyncio.sleep(5)  # 每5秒更新一次
    
    async def crawl(self):
        """开始爬取网站"""
        # 初始化连接和数据结构
        self.init_redis()
        await self.init_mongo()
        await self.init_session()
        
        self.start_time = time.time()
        self.crawl_running = True
        
        # 启动统计信息更新任务
        stats_task = asyncio.create_task(self.update_stats())
        
        # 初始化进度条
        self.progress_bar = tqdm(desc=f"爬取进度 (Worker: {self.worker_id})", unit="页")
        
        # 任务集合
        pending_tasks = set()
        max_concurrent_tasks = 50
        
        try:
            # 只要爬虫运行且(队列不为空或有任务正在处理)
            while self.crawl_running:
                # 更新心跳
                self.update_heartbeat()
                
                # 当任务数少于最大并发数时，添加新任务
                while len(pending_tasks) < max_concurrent_tasks:
                    url = await self.get_next_url()
                    if not url:
                        # 队列为空，等待一段时间
                        if not pending_tasks:
                            # 检查其他worker是否还在添加URL
                            if self.redis.scard(self.worker_key) <= 1 and self.redis.llen(self.queue_key) == 0:
                                # 我们是最后一个worker且队列为空，考虑退出
                                if self.processed_count > 0:  # 确保不是刚启动就退出
                                    self.crawl_running = False
                                    break
                        
                        # 等待新URL或其他任务完成
                        await asyncio.sleep(0.5)
                        break
                    
                    task = asyncio.create_task(self.process_url(url))
                    pending_tasks.add(task)
                    # 为任务添加回调，以便从集合中移除
                    task.add_done_callback(lambda t: pending_tasks.discard(t))
                
                # 如果任务集合不为空，等待任务完成
                if pending_tasks:
                    # 等待任意一个任务完成
                    done, _ = await asyncio.wait(
                        pending_tasks, 
                        return_when=asyncio.FIRST_COMPLETED,
                        timeout=1  # 添加超时，使循环能定期检查状态
                    )
                    
                    # 更新进度条总数
                    total = self.redis.llen(self.queue_key) + len(pending_tasks) + self.processed_count
                    if self.progress_bar.total != total:
                        self.progress_bar.total = total
                else:
                    # 没有任务但爬虫仍在运行，短暂等待
                    await asyncio.sleep(0.5)
        finally:
            # 取消统计任务
            if not stats_task.done():
                stats_task.cancel()
                try:
                    await stats_task
                except asyncio.CancelledError:
                    pass
            
            # 关闭资源
            if self.progress_bar is not None:
                self.progress_bar.close()
            
            # 从worker集合中移除自己
            self.redis.srem(self.worker_key, self.worker_id)
            self.redis.hdel(f"{self.worker_key}:info", self.worker_id)
            self.redis.delete(f"{self.worker_key}:heartbeat:{self.worker_id}")
            
            # 最后一次更新统计信息
            if self.db:
                await self.db.stats.insert_one({
                    "timestamp": time.time(),
                    "worker_id": self.worker_id,
                    "final": True,
                    "processed_count": self.processed_count,
                    "error_count": self.error_count,
                    "duration": time.time() - self.start_time
                })
            
            await self.close_session()
            await self.close_mongo()
    
    async def export_results(self, filename):
        """将结果导出到文件"""
        await self.init_mongo()
        
        with open(filename, 'w', encoding='utf-8') as f:
            async for doc in self.db.urls.find(
                {"status": 200}, 
                {"url": 1, "title": 1}
            ).sort("url", 1):
                title = doc.get("title", "")
                f.write(f"{doc['url']} - {title}\n")
                
        await self.close_mongo()
    
    async def count_results(self):
        """计算结果数量"""
        await self.init_mongo()
        count = await self.db.urls.count_documents({"status": 200})
        await self.close_mongo()
        return count
    
    async def run(self):
        """运行爬虫"""
        if self.base_domain is None and self.start_url:
            self.set_domain(urlparse(self.start_url).netloc)
        
        if self.base_domain is None:
            raise ValueError("必须提供起始URL或域名")
            
        start_time = time.time()
        await self.crawl()
        elapsed_time = time.time() - start_time
        
        # 显示统计信息
        print(f"\n爬虫工作完成！")
        print(f"工作节点ID: {self.worker_id}")
        print(f"处理URL数量: {self.processed_count}")
        print(f"错误数量: {self.error_count}")
        print(f"用时: {elapsed_time:.2f} 秒")
        
        # 只有当这是唯一的worker或最后一个worker时才导出结果
        if self.redis.scard(self.worker_key) == 0:
            print("这是最后一个工作节点，正在生成报告...")
            total_urls = await self.count_results()
            print(f"总共爬取有效URL: {total_urls}")
            
            output_file = f"{self.base_domain}_links.txt"
            await self.export_results(output_file)
            print(f"结果已保存到 {output_file}")
        else:
            print(f"其他工作节点仍在运行，当前共有 {self.redis.scard(self.worker_key)} 个活跃节点")
            

async def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="分布式网站爬虫")
    parser.add_argument("url", nargs="?", help="起始URL")
    parser.add_argument("--domain", help="要爬取的域名(如果不提供URL)")
    parser.add_argument("--redis-host", default="localhost", help="Redis主机地址")
    parser.add_argument("--redis-port", type=int, default=6379, help="Redis端口")
    parser.add_argument("--redis-db", type=int, default=0, help="Redis数据库编号")
    parser.add_argument("--mongo-uri", default="mongodb://localhost:27017", help="MongoDB连接URI")
    parser.add_argument("--mongo-db", help="MongoDB数据库名称")
    parser.add_argument("--connections", type=int, default=100, help="最大并发连接数")
    parser.add_argument("--worker-id", help="指定工作节点ID")
    parser.add_argument("--verify-ssl", action="store_true", help="是否验证SSL证书")
    
    args = parser.parse_args()
    
    if not args.url and not args.domain:
        parser.error("必须提供起始URL或域名")
    
    crawler = DistributedCrawler(
        start_url=args.url,
        worker_id=args.worker_id,
        max_connections=args.connections,
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        redis_db=args.redis_db,
        mongo_uri=args.mongo_uri,
        mongo_db=args.mongo_db,
        verify_ssl=args.verify_ssl
    )
    
    if args.domain and not args.url:
        crawler.set_domain(args.domain)
        
    await crawler.run()


if __name__ == "__main__":
    asyncio.run(main()) 