"""
获取日线行情数据
"""
import json
import pandas as pd
import numpy as np
import requests as rq
from quote.config import header


class KLine:
    def __init__(self):
        pass

    @staticmethod
    def get_a_universe():
        """
        爬取股票池信息
        :return:
        """
        univ_url = "http://43.push2.eastmoney.com/api/qt/clist/get?&pn={0}" \
                   "&pz=20&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:0+t:6,m:0+t:13," \
                   "m:0+t:80,m:1+t:2,m:1+t:23&fields=f14,f12&_=1581074944425"

        univ = list()

        page_content = rq.get(univ_url.format(1), headers=header)
        if page_content.status_code != 200:
            raise Exception('抓取失败')
        else:
            # 1. 获取总共多少只股票
            page_content = page_content.json()
            all_page = int(np.ceil(page_content['data']['total'] / 20.))

            univ.append(pd.DataFrame(page_content['data']['diff']))

            # 2. 遍历所有页面
            for i in range(2, all_page + 1):
                page_content = rq.get(univ_url.format(i), headers=header)
                page_content = page_content.json()
                univ.append(pd.DataFrame(page_content['data']['diff']))

        univ = pd.concat(univ)
        univ.columns = ['股票代码', '中文简称']

        return univ

    @staticmethod
    def get_stock_hist_quote(ticker, begin, end, adjust=False):
        """
        获取单只股票历史K线
        :param ticker:
        :return:
        """
        if not adjust:
            url = "http://57.push2his.eastmoney.com/api/qt/stock/kline/get?&secid={1}.{0}" \
                  "&ut=fa5fd1943c7b386f172d6893dbfba10b&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5" \
                  "&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58&klt=101&fqt=0&beg={2}&end={3}&smplmt=460&lmt=1000000"
        else:
            url = "http://89.push2his.eastmoney.com/api/qt/stock/kline/get?&secid={1}.{0}" \
                  "&ut=fa5fd1943c7b386f172d6893dbfba10b&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5" \
                  "&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58&klt=101&fqt=2&beg={2}&end={3}&smplmt=460&lmt=1000000"
        if ticker[0] == '6':
            page_content = rq.get(url.format(ticker, 1, begin, end), headers=header)
        else:
            page_content = rq.get(url.format(ticker, 0, begin, end), headers=header)

        kline = pd.DataFrame([i.split(',')[:7] for i in page_content.json()['data']['klines']])
        kline.columns = ['trade_date', 'open', 'close', 'high', 'low', 'volume', 'value']

        return kline

    @staticmethod
    def get_stock_in_day_quote(ticker):
        url = "http://pdfm.eastmoney.com/EM_UBG_PDTI_Fast/api/js?token=4f1862fc3b5e77c150a2b985b12db0fd&id={0}{1}&" \
              "type=r&iscr=false&rtntype=5"

        if ticker[0] == '6':
            page_content = rq.get(url.format(ticker, 1), headers=header)
        else:
            page_content = rq.get(url.format(ticker, 2), headers=header)

        kline = json.loads(page_content.text[1:-1])
        kline = pd.DataFrame([i.split(',')[:4] for i in kline['data']])
        kline.columns = ['trade_time', 'close', 'volume', 'vwap']

        kline = kline.set_index('trade_time').applymap(np.float)
        return kline


if __name__ == '__main__':
    a = KLine.get_stock_hist_quote('000001', '20200101', '20200228', adjust=False)