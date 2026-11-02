"""
toolbox/core_crawler.py
功能 2：多线程爬虫 —— 抓 ssr1.scrape.center 的电影信息（纯算法层）

============================================================
原理：爬虫到底在干什么
============================================================
所谓爬虫，本质就是"用程序代替人去浏览网页"，只有三步：
    ① 发请求  —— 程序替浏览器发出 HTTP GET，拿回一段 HTML 文本
    ② 拿响应  —— 服务器返回的 HTML，就是你在浏览器里"查看源代码"看到的东西
    ③ 抠数据  —— 从一大串 HTML 里，把想要的字段挑出来

和你在浏览器里按 F12 看 Network 的唯一区别是：
浏览器拿到 HTML 后会**渲染**成好看的页面；爬虫拿到的是**原始文本**，得自己解析。

============================================================
本模块的两个设计点
============================================================
1. 为什么要用多线程
   抓 10 个列表页 + 100 个详情页 = 110 次网络请求。
   一次请求的大部分时间花在"等服务器回应"上（网络往返通常几十到几百毫秒），
   CPU 这时候完全是闲着的。串行跑 110 次 = 把 110 段等待时间**相加**；
   开 8 个线程同时跑 = 等待时间**重叠**，总耗时大约是串行的 1/8。
   这就是 I/O 密集型任务（等网络/等磁盘）适合多线程的原因。

2. 为什么只抓这一个站
   ssr1.scrape.center 是崔庆才老师专门搭的**爬虫练习站**，就是给人练手的。
   对真实网站下手前必须看两件事：
     - robots.txt（网站声明哪些路径不许爬）
     - 对方的服务条款
   另外要控制频率——爬虫太猛会把人家网站拖垮，那就不叫练习，叫攻击了。
   本模块默认只抓 10 页 / 100 条，且线程数有上限，就是这个道理。
============================================================
"""
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

BASE = "https://ssr1.scrape.center"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36")
TOTAL_PAGES = 10          # 这个站一共就 10 页，100 部电影
PER_PAGE = 10

# ---- 解析用的正则（目标站点是服务端渲染，HTML 结构固定）----
RE_ITEM = re.compile(r'<div[^>]*class="el-card item')          # 用来"切"出每一部电影
RE_HREF = re.compile(r'href="/detail/(\d+)"')
RE_COVER = re.compile(r'<img[^>]*?src="([^"]+)"[^>]*?class="cover"', re.S)
RE_TITLE = re.compile(r'<h2[^>]*class="m-b-sm"[^>]*>(.*?)</h2>', re.S)
RE_CATE = re.compile(r'<button[^>]*class="el-button category[^"]*"[^>]*>\s*'
                     r'<span>(.*?)</span>', re.S)
RE_INFO = re.compile(r'<div[^>]*class="m-v-sm info"[^>]*>(.*?)</div>', re.S)
RE_SPAN = re.compile(r'<span[^>]*>(.*?)</span>', re.S)
RE_SCORE = re.compile(r'<p[^>]*class="score[^"]*"[^>]*>\s*([0-9.]+)', re.S)
RE_DRAMA = re.compile(r'<div[^>]*class="drama"[^>]*>.*?<p[^>]*>(.*?)</p>', re.S)
RE_DATE = re.compile(r'(\d{4}-\d{2}-\d{2})')


def fetch(url: str, timeout: float = 15.0, retries: int = 2) -> str:
    """
    发一次 GET 请求，带上 UA 伪装，失败自动重试。
    重试间隔递增（0.5s / 1.0s），避免把对方服务器冲垮。
    """
    last = "未知错误"
    for i in range(retries + 1):
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=timeout)
            if r.status_code == 200:
                r.encoding = "utf-8"
                return r.text
            last = f"HTTP {r.status_code}"
        except requests.Timeout:
            last = "请求超时"
        except Exception as e:
            last = f"{type(e).__name__}"
        if i < retries:
            time.sleep(0.5 * (i + 1))
    raise RuntimeError(last)


def _clean(s: str) -> str:
    """去掉 HTML 标签和多余空白（正则抠出来的文本常常带着换行和标签）。"""
    s = re.sub(r"<[^>]+>", "", s or "")
    return re.sub(r"\s+", " ", s).strip()


def parse_list(html: str):
    """
    解析一个列表页，返回这一页的电影列表。

    页面结构（每部电影一块）：
        <div class="el-card item ...">
            <img src="...jpg" class="cover">              海报
            <a href="/detail/1" ...><h2 class="m-b-sm">霸王别姬 - Farewell...</h2></a>
            <button class="el-button category"><span>剧情</span></button>   分类（可多个）
            <div class="m-v-sm info"><span>中国内地</span><span> / </span><span>171 分钟</span></div>
            <div class="m-v-sm info"><span>1993-07-26 上映</span></div>
            <p class="score ...">9.5</p>                   评分
    """
    out = []
    # 用"每块的开头标记"把整页切成 10 段，每段是一部电影
    for block in RE_ITEM.split(html)[1:]:
        m_id = RE_HREF.search(block)
        m_title = RE_TITLE.search(block)
        if not (m_id and m_title):
            continue

        title = _clean(m_title.group(1))
        # 标题形如 "霸王别姬 - Farewell My Concubine"，拆成中文名 + 英文名
        if " - " in title:
            cn, en = title.split(" - ", 1)
        else:
            cn, en = title, ""

        # info 区里混着地区 / 时长 / 上映日期，按特征分辨，不依赖顺序
        region = duration = publish = ""
        for info in RE_INFO.findall(block):
            for sp in RE_SPAN.findall(info):
                t = _clean(sp)
                if not t or t == "/":
                    continue
                if "分钟" in t and not duration:
                    duration = t
                elif RE_DATE.search(t) and not publish:
                    publish = RE_DATE.search(t).group(1)
                elif not region:
                    region = t

        m_cover = RE_COVER.search(block)
        m_score = RE_SCORE.search(block)

        out.append({
            "id": m_id.group(1),
            "name_cn": cn.strip(),
            "name_en": en.strip(),
            "categories": "/".join(_clean(c) for c in RE_CATE.findall(block)),
            "region": region,
            "duration": duration,
            "publish": publish,
            "score": m_score.group(1).strip() if m_score else "",
            "cover": m_cover.group(1).strip() if m_cover else "",
            "drama": "",                       # 详情页才有，后面补
            "url": f"{BASE}/detail/{m_id.group(1)}",
        })
    return out


def parse_detail(html: str) -> str:
    """解析详情页，只要剧情简介（其他字段列表页已经全有了）。"""
    m = RE_DRAMA.search(html)
    return _clean(m.group(1)) if m else ""


def crawl(pages: int = TOTAL_PAGES, workers: int = 8, timeout: float = 15.0,
          with_detail: bool = False,
          progress_cb=None, result_cb=None, stop_event=None):
    """
    完整爬取流程：先列表页，再（可选）详情页。

    回调风格与 core_scan / core_ip / core_subdomain 完全一致：
        progress_cb(done, total)   每完成一个请求报一次进度
        result_cb(movie)           每解析出一部电影就回调（界面可以实时一行行冒出来）
        stop_event                 threading.Event，点停止时置位

    返回全部电影列表。
    """
    pages = max(1, min(int(pages), TOTAL_PAGES))
    workers = max(1, min(int(workers), 16))     # 上限 16，别把练习站冲垮

    list_urls = [f"{BASE}/page/{p}" for p in range(1, pages + 1)]
    grand = len(list_urls) + (pages * PER_PAGE if with_detail else 0)
    done = 0
    movies = []

    def stopped():
        return bool(stop_event and stop_event.is_set())

    # ---------- 阶段 1：抓列表页 ----------
    def get_list(url):
        if stopped():
            return []
        return parse_list(fetch(url, timeout=timeout))

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(get_list, u) for u in list_urls]
        for f in as_completed(futures):
            if stopped():
                break
            try:
                movies.extend(f.result())
            except Exception:
                pass                    # 单页失败不拖垮整体
            done += 1
            if progress_cb:
                progress_cb(done, grand)

    if stopped():
        return movies

    # 多线程返回顺序是乱的，按 id 排好再统一抛给界面（保证表格顺序稳定）
    movies.sort(key=lambda m: int(m["id"]) if m["id"].isdigit() else 0)
    for m in movies:
        if result_cb:
            result_cb(m)

    if not with_detail:
        return movies

    # ---------- 阶段 2：抓详情页补剧情简介（补齐一行就更新一行）----------
    def get_detail(movie):
        if stopped():
            return None
        try:
            movie["drama"] = parse_detail(fetch(movie["url"], timeout=timeout))
        except Exception:
            movie["drama"] = ""
        return movie

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(get_detail, m) for m in movies]
        for f in as_completed(futures):
            if stopped():
                break
            r = f.result()
            done += 1
            if progress_cb:
                progress_cb(done, grand)
            if r is not None and result_cb:
                result_cb(r)            # 同一 id，界面上是"覆盖刷新"那一行

    return movies


def save_csv(movies, path: str):
    """把结果存成 CSV（Excel 能直接打开，注意用 utf-8-sig 否则中文乱码）。"""
    import csv
    fields = ["id", "name_cn", "name_en", "score", "categories",
              "region", "duration", "publish", "url", "cover", "drama"]
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for m in movies:
            w.writerow(m)
    return path
