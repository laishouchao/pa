# 高效网站链接爬虫

这是一个高效的网站链接爬虫程序，它可以从指定的起始URL开始，自动访问并生成网站内所有网址的列表。该程序具有以下特点：

- 异步并发爬取，极大提高扫描速度
- 自动处理相对路径链接（如 `href="/xxx"` 和 `src="/_sitegray/_sitegray.js"` 形式）
- 确保生成的链接列表无重复
- 高效的内存使用
- 实时显示爬取进度
- 支持忽略SSL证书验证，可爬取有证书问题的HTTPS网站
- 可抓取JavaScript和CSS等资源文件

## 依赖项

- Python 3.7+
- aiohttp
- beautifulsoup4
- tqdm

## 安装

1. 克隆或下载此仓库
2. 安装依赖项：

```bash
pip install -r requirements.txt
```

## 使用方法

### 基本版本

```bash
python web_crawler.py <起始URL> [verify_ssl]
```

例如：

```bash
# 默认不验证SSL证书，可处理证书问题的网站
python web_crawler.py https://example.com

# 验证SSL证书（安全但可能无法访问证书有问题的网站）
python web_crawler.py https://example.com true
```

### Redis版本（高效的分布式爬取）

```bash
python web_crawler_redis.py <起始URL> [Redis主机 [Redis端口 [Redis数据库 [verify_ssl]]]]
```

例如：

```bash
# 默认配置
python web_crawler_redis.py https://example.com

# 完整配置
python web_crawler_redis.py https://example.com localhost 6379 0 false
```

### MongoDB版本（存储更多元数据）

```bash
python web_crawler_mongo.py <起始URL> [MongoDB URI [数据库名 [verify_ssl]]]
```

例如：

```bash
# 默认配置
python web_crawler_mongo.py https://example.com

# 完整配置
python web_crawler_mongo.py https://example.com mongodb://localhost:27017 my_crawler_db false
```

### 分布式版本（大规模爬取）

```bash
python web_crawler_distributed.py <起始URL> [参数选项]
```

支持的参数选项：
- `--domain` - 要爬取的域名(如果不提供URL)
- `--redis-host` - Redis主机地址
- `--redis-port` - Redis端口
- `--redis-db` - Redis数据库编号
- `--mongo-uri` - MongoDB连接URI
- `--mongo-db` - MongoDB数据库名称
- `--connections` - 最大并发连接数
- `--worker-id` - 指定工作节点ID
- `--verify-ssl` - 是否验证SSL证书（默认不验证）

例如：

```bash
# 不验证SSL证书（默认）
python web_crawler_distributed.py https://example.com

# 验证SSL证书
python web_crawler_distributed.py https://example.com --verify-ssl
```

## 输出

程序会生成一个文本文件，包含爬取到的所有网址。文件名格式为：`域名_links.txt`。

## 性能

- 程序默认最大并发连接数为100，可以根据需要修改代码中的`max_connections`参数
- 默认请求超时时间为10秒，可以根据需要修改代码中的`timeout`参数
- 默认不验证SSL证书，可以处理证书有问题的HTTPS网站

## 注意事项

- 此程序仅爬取同一域名下的链接
- 会自动过滤图片、PDF、ZIP等媒体资源，但保留JavaScript和CSS文件
- 同时抓取`href`和`src`属性中的URL链接
- 请合理使用，避免对目标服务器造成过大负担
- 如果目标网站有SSL证书问题，程序默认会忽略并继续爬取 