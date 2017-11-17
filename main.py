# -*- coding: UTF-8 -*-
# --- main.py -----
#嘉祥云视频抓取爬虫


__author__ = 'Derek.S'

import types
import MySQLdb
import urllib2
import json
import sys
import re
import io
import string
import time
import threading
from colorama import init,Fore,Back,Style
from termcolor import colored,cprint
import progressbar


from pprint import pprint
import time

#数据库连接
db = MySQLdb.connect("192.168.10.105","root","123a+-","ssxy")
c = db.cursor()
db.set_character_set('utf8')
#初始化colorama
init(autoreset=True)


def IDTest(VideoID):
    "重复ID检测函数，VideoID = 视频ID"
    print Fore.YELLOW + '|--------重复ID检测---------|'
    c.execute('select lesson_id from Video where lesson_id = "%d"'%(VideoID))
    test = c.fetchall()
    if(len(test) == 1):
        if(test[0][0]):
            cprint ('|---------检测到重复ID-------------|','red')
            return(True)
        else:
            cprint ('|--------未检测到重复ID------------|','blue')
            return(False)

def TimeOutTest(Url):
    "URL超时检测，Url = url"
    req = urllib2.Request(Url)
    fails = 1
    while fails < 31:
        try:
            urlopen = urllib2.urlopen(Url,timeout=60)
            break
        except:
            print Fore.RED + '-----访问超时，第 ',fails,' 次重试------'
            fails += 1
    else:
        print Fore.RED + '----- 重试失败，访问超时 ------'
        raise
    return(urlopen)

def StrRep(Lesson_Name):
    "视频名称异常字符检测，Lesson_Name=str"
    if(Lesson_Name.find("\\") != -1):
        print Fore.RED + '----- 文件名内存在特殊字符，替换处理 -----'
        rname = Lesson_Name.replace("\\","")
        print '处理后名称：',rname
    elif(Lesson_Name.find("\"") != -1):
        print Fore.RED + '----- 文件名内存在特殊字符，替换处理 -----'
        rname = Lesson_Name.replace("\"","")
    else:
        rname = Lesson_Name
    return(rname)

def FileSize(url):
    "视频文件大小检测，url=url"
    try:
        opener = urllib2.build_opener()
        request = urllib2.Request(url)
        request.get_method = lambda: 'HEAD'
        response = opener.open(request)
        response.read()
        size = int(dict(response.headers).get('content-length',0))/1024/1024
        return size
    except Exception,e:
        print '----- 文件不存在 404 Not Found -----'
        notfound = 'yes'
        return notfound


def VideoMain(FirstID,EndID):
    "范围ID抓取函数，FirstID为起始ID，EndID为截止ID"
    videoamonut = 0
    videolist = []
    while(FirstID < EndID):
        #重复ID检测
        if (IDTest(FirstID)):
            FirstID += 1
            continue
        else:
            u = 'http://www.hiteacher.com.cn/%s'%FirstID
            print '检测ID：',FirstID
            print '检测URL：',u
            html = TimeOutTest(u).read() #重复ID检测，返回urlopen
            resp = json.loads(html)
            sub = resp['video_list']
            if(len(sub)!=1):
                print Fore.RED + '------- 该ID尚未启用，写入未用ID表 -------'
                c.execute('insert into Unused(lesson_id) values("%s")' % (FirstID))
                db.commit()
            else:
                lesson_id = FirstID
                print '课程ID：',lesson_id
                lesson_name = sub[0]['name']
                print '课程名称：',lesson_name
                video_type = sub[0]['type']
                print '类型：', video_type
                video_format = sub[0]['format']
                print '格式：', video_format
                server = sub[0]['server_ip']
                print '服务器地址：',server
                path = sub[0]['path']
                print '路径：',path
                rname = StrRep(lesson_name) #文件名特殊字符检测
                video_url = server+path
                file = FileSize(video_url) #文件大小检测
                if(file != 'yes'):
                    print Fore.WHITE + '----- 文件大小：',str(file)+'M -----'
                    print Fore.BLUE + '----- 数据写入数据库 -----'
                    c.execute('insert into Video(lesson_id,name,type,format,server,path,size) values("%s","%s","%s","%s","%s","%s","%s")'%(lesson_id,rname,video_type,video_format,server,path,file))
                    db.commit()
                    videoamonut += 1
                    videolist.append(rname)
                elif(file == 'yes'):
                    notfound = 'yes'
                    c.execute('insert into Notfound(lesson_id,name,server,path,notfound) values("%s","%s","%s","%s","%s")'%(lesson_id,rname,server,path,notfound))
                    db.commit()
            FirstID += 1
    print '新视频抓取完成，共抓取 ',videoamonut,' 节'
    print json.dumps(videolist,encoding='utf-8',ensure_ascii=False,indent=1)
    db.close()

def Unused():
    c.execute('select lesson_id from Unused')
    id = c.fetchall()
    c.execute('select count(*) from Unused')
    row = c.fetchall()
    totalrow = row
    print '共有未用ID：',totalrow[0][0],'个'
    used = 0
    videolist = []
    i = 0
    while(i < row[0][0]):
        name = Unusedid(id[i][0])
        i += 1
        videolist.append(name)
    used = len(videolist)
    print '未用ID检测完成，共更新抓取 ',used,'条记录'
    print json.dumps(videolist,encoding='utf-8',ensure_ascii=False,indent=1)

def Unusedid(Unid):
    u = 'http://www.hiteacher.com.cn/%s' % Unid
    print '检测ID：',Unid
    print '检测URL：',u
    html = TimeOutTest(u).read() #重复ID检测，返回urlopen
    resp = json.loads(html)
    sub = resp['video_list']
    if(len(sub)!=1):
        print '该ID仍未启用，跳过'
    else:
        lesson_id = Unid
        print '课程ID：',lesson_id
        lesson_name = sub[0]['name']
        print '课程名称：',lesson_name
        video_type = sub[0]['type']
        print '类型：', video_type
        video_format = sub[0]['format']
        print '格式：', video_format
        server = sub[0]['server_ip']
        print '服务器地址：',server
        path = sub[0]['path']
        print '路径：',path
        rname = StrRep(lesson_name) #文件名特殊字符检测
        video_url = server+path
        file = FileSize(video_url) #文件大小检测
        if(file != 'yes'):
            print Fore.WHITE + '----- 文件大小：',str(file)+'M -----'
            print Fore.BLUE + '----- 数据写入数据库 -----'
            c.execute('insert into Video(lesson_id,name,type,format,server,path,size) values("%s","%s","%s","%s","%s","%s","%s")'%(lesson_id,rname,video_type,video_format,server,path,file))
            db.commit()
            print Fore.BLUE + '----- 删除Unused表内对应记录 -----'
            c.execute('delete from Unused where lesson_id = "%s"'%(Unid))
            db.commit()
            if rname != '':
                return rname
        elif(file == 'yes'):
            notfound = 'yes'
            c.execute('insert into Notfound(lesson_id,name,server,path,notfound) values("%s","%s","%s","%s","%s")'%(lesson_id,rname,server,path,notfound))
            db.commit()
    #db.close()

def Downinfo(Tidfrist,num,path):
    "下载辅助模块，idfirst = 数据表起始ID，num = 抓取数量"
    Tidend = int(Tidfrist) + int(num)
    while(Tidfrist < Tidend):
        c.execute('select * from Video where id = "%s"'%(Tidfrist))
        info = c.fetchall()
        d_filename = str(info[0][2])
        d_url = str(info[0][5])+str(info[0][6])
        print d_filename
        print d_url
        Tidfrist += 1
        Download(d_url,path,d_filename)

def Download(url,path,name):
    "数据下载模块，url=下载地址 path=存储地址 name=文件名"
    filepath = path
    filename = name + '.mp4'
    fullpath = filepath + filename
    u = urllib2.urlopen(url)
    f = open(fullpath,'wb')
    meta = u.info()
    file_size = int(meta.getheaders("Content-Length")[0])
    cprint ('正在下载：%s 文件大小：%s'%(filename,file_size),'white',attrs=['blink'])

    file_size_dl = 0
    block_size = 8192
    #progress = ProgressBar().start()
    widgets = [progressbar.Percentage(), progressbar.Bar()]
    bar = progressbar.ProgressBar(widgets=widgets, max_value=100).start()
    while True:
        buffer = u.read(block_size)
        if not buffer:
            break

        file_size_dl += len(buffer)
        f.write(buffer)
        bar.update(file_size_dl*100/file_size)
        #time.sleep(0.02)
    bar.finish()
    f.close
    cprint ('下载完成','white')

#交互界面

print '-----------------------------------------------------------'
print '-                 嘉祥云视频信息抓取分析工具              -'
print '-----------------------------------------------------------'
print ''
print '操作列表：'
print '1.设置视频ID范围并进行抓取'
print '2.抓取未用ID表内的视频信息'
print '3.下载视频'
print 'Q.退出'

#交互界面响应
operate = input('请输入需要进行的操作：')
if(operate == 1):
    c.execute('select lesson_id from Video order by id desc limit 1')
    lastid = c.fetchall()
    c.execute('select lesson_id from Unused order by id desc limit 1')
    unlastid = c.fetchall()
    print '-----------------------------------------------------------'
    print '-                   视频ID范围抓取                  -'
    print '-当前最后一次抓取到的课程ID为：%s'%(lastid[0][0])
    print '-当前未用数据表内最大未用ID为 %s'%(unlastid[0][0])
    print '-----------------------------------------------------------'
    idfrist = input('请输入抓取起始ID：')
    idend = input('请输入抓取截止ID：')
    VideoMain(idfrist,idend)
elif(operate == 2):
    print '-----------------------------------------------------------'
    print '-                   未用ID范围抓取                  -'
    print '-----------------------------------------------------------'
    Unused()
elif(operate == 3):
    
    c.execute('select * from Video limit 1')
    frow = c.fetchall()[0][0]
    c.execute('select count(*) from Video')
    trows = c.fetchall()[0][0]
    f = open('download-log.log','a+')
    log = f.read()
    lastid = log
    f.close
    d_path = '/home/derek/ssxy_video/'
    print '-----------------------------------------------------------'
    print '-                      视频下载                            -'
    print '-----------------------------------------------------------'
    print '- 最近一次下载的起始结束ID与数量分别为：'
    print '- %s'%(lastid)
    print '- 若要查询历次抓取数据，请参考本脚本文件夹内的log文件              '
    print '- 默认的下载地址为：/home/derek/ssxy_video'
    print '- 当前数据表内首行ID为：%s                            '%(frow)
    print '- 当期数据表内共计',trows,'行'                   
    print '-----------------------------------------------------------' 
    d_id = input('抓取的起始ID：')
    d_num = input('抓取数量：')
    d_pathif = raw_input('修改下载地址(默认为：/home/derek/ssxy_video)若不修改，请直接回车跳过：')
    d_tol = int(d_id)+int(d_num)
    f = open('download-log.log','w')
    f.write(str(d_id)+","+str(d_tol)+","+str(d_num)+"\n")
    f.close()
    f = open('download-log-total.log','a+')
    f.write(str(d_id)+","+str(d_tol)+","+str(d_num)+"\n")
    f.close
    if not d_pathif:
        print '未修改下载路径，视频将下载之默认目录中'
        Downinfo(d_id,d_num,d_path)
    else:
        print d_pathif
        print 'error'
        
elif(operate == 'Q'):
    print Fore.WHITE + '------------ bye --------------'
    sys.exit()
                

                
