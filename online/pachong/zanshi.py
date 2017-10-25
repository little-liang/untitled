#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import re
from io import StringIO, BytesIO
from PIL import Image
import random
from functools import reduce
import time
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
from bs4 import BeautifulSoup
import sys
import MySQLdb


class mysql(object):
    """
    mysql 操作

    """

    def __init__(self):
        pass

    def common_run_sql(self, sql):
        config_info_json = {
            'host': "60.194.156.184",
            'port': 3123,
            'db': 'geetest_02',
            'user': 'test101',
            'password': 'test101'}
        self.conn = MySQLdb.connect(**config_info_json, charset='utf8')
        self.cursor = self.conn.cursor()

        ## 待优化 每次查询都要连接断开,假设连接断开很慢,
        ## 最好本次程序运行完毕在进行数据库断开操作
        ##main 做成 上下文??
        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            return result
        finally:
            self.conn.commit()

    def __del__(self):
        self.cursor.close()
        self.conn.close()


class crack_picture(object):
    def __init__(self, img_url1, img_url2):
        self.img1, self.img2 = self.picture_get(img_url1, img_url2)

    def picture_get(self, img_url1, img_url2):
        hd = {"Host": "static.geetest.com",
              "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36"}

        img1 = BytesIO(self.repeat(img_url1, hd).content)
        img2 = BytesIO(self.repeat(img_url2, hd).content)
        return img1, img2

    def repeat(self, url, hd):
        times = 10
        while times > 0:
            try:
                ans = requests.get(url, headers=hd)
                return ans
            except:
                times -= 1

    def pictures_recover(self):
        xpos = self.judge(self.picture_recover(self.img1, 'img1.jpg'), self.picture_recover(self.img2, 'img2.jpg')) - 6
        return self.darbra_track(xpos)

    def picture_recover(self, img, name):
        a = [39, 38, 48, 49, 41, 40, 46, 47, 35, 34, 50, 51, 33, 32, 28, 29, 27, 26, 36, 37, 31, 30, 44, 45, 43, 42, 12,
             13, 23, 22, 14, 15, 21, 20, 8, 9, 25, 24, 6, 7, 3, 2, 0, 1, 11, 10, 4, 5, 19, 18, 16, 17]
        im = Image.open(img)
        im_new = Image.new("RGB", (260, 116))
        for row in range(2):
            for column in range(26):
                right = a[row * 26 + column] % 26 * 12 + 1
                if a[row * 26 + column] > 25:
                    down = 58
                else:
                    down = 0
                for w in range(10):
                    for h in range(58):
                        ht = 58 * row + h
                        wd = 10 * column + w
                        im_new.putpixel((wd, ht), im.getpixel((w + right, h + down)))
        im_new.save(name)
        return im_new

    def darbra_track(self, distance):
        t = 1.0 / distance
        list = [[0, 0.5, t]]
        # print(distance)
        for x in range(0, distance - 1, 2):
            list.append([2, random.random() * 5, t])
        return distance
        # crucial trace code was deleted

    def diff(self, img1, img2, wd, ht):
        rgb1 = img1.getpixel((wd, ht))
        rgb2 = img2.getpixel((wd, ht))
        tmp = reduce(lambda x, y: x + y, map(lambda x: abs(x[0] - x[1]), zip(rgb1, rgb2)))
        return True if tmp >= 200 else False

    def col(self, img1, img2, cl):
        for i in range(img2.size[1]):
            if self.diff(img1, img2, cl, i):
                return True
        return False

    def judge(self, img1, img2):
        for i in range(img2.size[0]):
            if self.col(img1, img2, i):
                return i
        return -1


class gsxt(object):
    def __init__(self, br_name="phantomjs"):
        self.br = self.get_webdriver(br_name)
        self.br.set_window_size(width=800, height=600)
        self.wait = WebDriverWait(self.br, 20, 1.0)
        self.br.set_page_load_timeout(10)
        self.br.set_script_timeout(10)

    def input_params(self, name):
        self.br.get("http://www.gsxt.gov.cn/index")
        # print (self.br.page_source)
        element = self.wait_for(By.ID, "keyword")
        element.send_keys(name)
        time.sleep(0.3)
        element = self.wait_for(By.ID, "btn_query")
        element.click()
        time.sleep(0.6)

    def drag_pic(self):
        return (self.find_img_url(self.wait_for(By.CLASS_NAME, "gt_cut_fullbg_slice")),
                self.find_img_url(self.wait_for(By.CLASS_NAME, "gt_cut_bg_slice")))

    def wait_for(self, by1, by2):
        return self.wait.until(EC.presence_of_element_located((by1, by2)))

    def find_img_url(self, element):
        try:
            return re.findall('url\("(.*?)"\)', element.get_attribute('style'))[0].replace("webp", "jpg")
        except:
            return re.findall('url\((.*?)\)', element.get_attribute('style'))[0].replace("webp", "jpg")

    def emulate_track(self, tracks):
        element = self.br.find_element_by_class_name("gt_slider_knob")

        action = ActionChains(self.br)
        action.click_and_hold(on_element=element).perform()
        # print(tracks)

        total = 1200
        if tracks < 50:
            total = random.randint(600, 1600)
        elif tracks >= 50 and tracks < 100:
            total = random.randint(800, 1200)
        elif tracks >= 100 and tracks < 150:
            total = random.randint(1000, 1500)
        elif tracks >= 150 and tracks < 200:
            total = random.randint(1200, 1700)
        else:
            total = random.randint(1500, 2000)

        xroffset = -random.randint(0, 1)

        action.move_by_offset(xroffset, 0)

        deltat1 = random.randint(20, 60) / 1000

        # print(deltat1)
        time.sleep(deltat1)

        action.move_by_offset(-xroffset, 0)

        deltat3 = random.randint(100, 200) / 1000
        # print(deltat3)

        deltat2 = total - 1000 * (deltat1 + deltat3)
        # print("deltat2=%d"%deltat2)

        v0 = tracks * 2 / deltat2
        a = -v0 / deltat2

        deltat = random.randint(20, 50)
        prevSumt = 0
        totalDist = 0
        bFind = False

        # print("\n\n")
        ppp = 0
        while deltat2 - deltat >= 0:
            deltas = v0 * deltat + a * prevSumt * deltat + 0.5 * a * deltat * deltat
            action.move_by_offset(deltas, 0)
            totalDist += deltas
            # print(totalDist)
            # ppp += deltat
            # print(deltat)
            # print('ppp', ppp)
            # if tracks-totalD
            #     ist>10:
            #    time.sleep(deltat/10000)
            #            if random.randint(0, 1)==0 :
            #                xroffset=-random.randint(5, 10)
            #                action.move_by_offset(xroffset, 0)
            #                time.sleep(-xroffset/100)
            #                action.move_by_offset(-xroffset, 0)
            #            if tracks-totalDist<1 and random.randint(0, 1)==0:
            #                xoffset=int(tracks-totalDist)
            #                action.move_by_offset(xoffset+6, 0)
            #                time.sleep((xoffset+2)/100)
            #                #action.move_by_offset(-xoffset-1, 0)


            if tracks - totalDist < 2:
                xoffset = int(tracks - totalDist + 0.5)
                action.move_by_offset(xoffset + 4, 0)
                time.sleep((xoffset + 4) / 100)
                action.move_by_offset(5, 0)
                break
            prevSumt += deltat
            deltat2 -= deltat
            if deltat2 <= 45:
                if bFind == True:
                    break
                deltat = deltat2
                bFind = True
            else:
                deltat = random.randint(25, 45)
        time.sleep(deltat3)
        # action.move_by_offset(tracks-totalDist, 0)
        action.move_by_offset(2, 0)
        action.release(on_element=element).perform()
        time.sleep(0.8)
        element = self.wait_for(By.CLASS_NAME, "gt_info_text")
        ans = element.text.encode("utf-8")
        # print(ans)
        # print("\n\n")
        return ans

    def run(self):
        ###保持浏览器开启
        while True:

            ##数据库读出, 或者socket接收
            w_list = run_sql_obj.common_run_sql("select short_name from query where flag=0 limit 10")
            if len(w_list) == 0:
                print('没有待爬取的项目')
                time.sleep(10)
                continue

            ##查所有
            for company in w_list:
                company = company[0]

                while True:
                    ##如果未查或者未查完
                    sql = "select flag from result where name='%s' and (flag = '0' or flag ='1')" % (company)
                    flag = run_sql_obj.common_run_sql(sql)

                    sql = "select flag from result where name='%s'" % (company)
                    num_company = run_sql_obj.common_run_sql(sql)

                    if len(flag) == 0 and len(num_company) >= 1:
                        sql = "update query set flag=1 where short_name='%s'" % (company)
                        run_sql_obj.common_run_sql(sql)
                        break
                    else:
                        self.hack_geetest(company)



            time.sleep(10)

    def hack_geetest(self, company):
        flag = True
        self.input_params(company)
        while flag:
            img_url1, img_url2 = self.drag_pic()
            tracks = crack_picture(img_url1, img_url2).pictures_recover()
            tsb = self.emulate_track(tracks)

            if '通过' in tsb.decode():
                time.sleep(0.3)

                soup = BeautifulSoup(self.br.page_source, 'html.parser')
                all_a_lable = soup.find_all("a", attrs={"class": "search_list_item db"})

                if len(all_a_lable) == 0:
                    print(company, ' 这个名字啥也没有工商信息')
                    sql = "update query set flag=1 where short_name='%s'" % (company)
                    run_sql_obj.common_run_sql(sql)
                    break

                ##写每个子公司的url
                for sp in all_a_lable:
                    company_name = sp.h1.text
                    company_name = str(company_name).strip()
                    url_string = 'http://www.gsxt.gov.cn' + sp['href']

                    ##查完收工
                    import datetime
                    now_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

                    sql = "select fullname from result where fullname = '%s'" % (company_name)
                    res = run_sql_obj.common_run_sql(sql)

                    # print(len(res), sql, res, company_name)
                    if len(res) == 0:
                        # 写入表
                        now_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
                        sql = "insert into result(name,fullname,url,updatetime,flag) value('%s','%s','%s','%s','0')" % (
                        company, company_name, url_string, now_time)
                        run_sql_obj.common_run_sql(sql)
                    else:
                        now_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
                        sql = "update result set url = '%s', updatetime='%s' where fullname = '%s' " % (url_string,company_name, now_time)
                        print(sql)
                        res = run_sql_obj.common_run_sql(sql)

                break
            elif '吃' in tsb.decode():
                time.sleep(0.2)
            else:
                self.input_params(company)

    def quit_webdriver(self):
        self.br.quit()

    def get_webdriver(self, name):

        if name.lower() == "phantomjs":
            dcap = dict(DesiredCapabilities.PHANTOMJS)
            dcap["phantomjs.page.settings.userAgent"] = (
                "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3080.5 Safari/537.36")
            # return webdriver.PhantomJS(desired_capabilities=dcap)
            return webdriver.PhantomJS(executable_path=r"E:\tools\phantomjs-2.1.1-windows\bin\phantomjs.exe")

        elif name.lower() == "chrome":
            return webdriver.Chrome(r"E:\tools\chromedriver.exe")


if __name__ == "__main__":
    run_sql_obj = mysql()
    while True:
        try:
            a_obj = gsxt("phantomjs")
            a_obj.run()
        except Exception as e:
            print(str(e))
            a_obj.quit_webdriver()
            continue