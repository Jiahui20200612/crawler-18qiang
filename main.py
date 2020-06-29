import math
import asyncio
import argparse
import bs4
import re
import aiohttp

# 最大页(论坛限制)
limit_page = 800
limit_retry = 5


class TimeoutLock:
    """
    超时锁, 超过指定时间自动释放

    """
    def __init__(self, lock, timeout):
        self.lock = lock
        self.timeout = timeout

    async def acquire(self):
        """
        获取锁, 后台自动释放

        """
        if self.timeout == 0:
            return
        await self.lock.acquire()
        asyncio.create_task(self.release())

    async def release(self):
        await asyncio.sleep(self.timeout)
        self.lock.release()


async def get_post_id(pages, post_ids, client, lock):
    """
    获取帖子id添加到队列

    :param pages: 页码
    :param post_ids: 帖子id队列
    :param client: http客户端
    :param lock: 锁
    """

    while True:
        try:
            page = next(pages)
        except StopIteration:
            break

        url = f'https://www.18qiang.com/thread-htm-fid-2-page-{page}.html'
        for retry_count in range(limit_retry):
            await lock.acquire()
            async with client.get(url) as resp:
                if resp.status != 200:
                    continue
                html = await resp.text('gbk')
                break
        else:
            print(f'请求失败: {url}')
            continue

        print(url)
        for post_id in re.findall('<a href="read-htm-tid-(\d*).*?" name="readlink"', html):
            await post_ids.put(post_id)


async def get_post(post_ids, client, lock, file):
    """
    获取帖子信息并保存

    :param post_ids: 帖子id队列
    :param client: http客户端
    :param lock: 锁
    """
    while post_id := await post_ids.get():
        url = f'https://m.18qiang.com/read.php?tid={post_id}'
        for retry_count in range(limit_retry):
            await lock.acquire()
            async with client.get(url) as resp:
                if resp.status != 200:
                    continue
                html = await resp.text('gbk')
                break
        else:
            print(f'请求失败: {url}')
            continue

        post_soup = bs4.BeautifulSoup(html, 'html.parser')
        post = {
            'id': post_id,
            # 标题
            'title': post_soup.select_one('div.ui-list-title').find(text=True, recursive=False).strip(),
            # 内容
            'content': post_soup.select_one('div.detail').text.strip(),
            # 原始内容(包含所有标签)
            'content-raw': str(post_soup.select_one('div.detail')),
        }
        print(post, file=file)
        print(url)
    await post_ids.put(None)


async def main(start, end, concurrent, qps, outfile):
    pages = (page for page in range(start, end + 1))
    post_ids = asyncio.queues.Queue()
    lock = TimeoutLock(asyncio.Lock(), qps if qps == 0 else 1 / qps)

    async with aiohttp.ClientSession() as client:
        with open(outfile, 'w') as file:
            # 1比40
            page_task_max = math.ceil(concurrent * (1/40))
            post_task_max = concurrent - page_task_max

            page_task = asyncio.gather(*(get_post_id(pages, post_ids, client, lock) for _ in range(page_task_max)))
            post_task = asyncio.gather(*(get_post(post_ids, client, lock, file) for _ in range(post_task_max)))
            await page_task
            await post_ids.put(None)
            await post_task


if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument('-o', '--outfile', type=str, help='输出文件', metavar='', required=True)
    args.add_argument('-s', '--start', default=1, type=int, help='开始页, 默认1', dest='start', metavar='')
    args.add_argument('-e', '--end', default=limit_page, type=int, help=f'结束页, 默认最大{limit_page}', metavar='')
    args.add_argument('-c', '--concurrent', default=4, type=int, help='并发数, 最小2', metavar='')
    args.add_argument('-q', '--qps', default=0, type=int, help='每秒请求数', metavar='')
    args = args.parse_args()

    # 检查参数
    if (args.start < 1 or args.start > limit_page) or \
            (args.end < args.start or args.end > limit_page) or \
            (args.concurrent < 2) or \
            (args.qps < 0):
        exit('参数错误')

    # 检查文件
    try:
        with open(args.outfile, 'w'):
            pass
    except OSError:
        exit('打开文件失败')

    asyncio.run(main(args.start, args.end, args.concurrent, args.qps, args.outfile))
