# -*- coding: utf-8 -*-
# @Time : 2019/11/12 10:30
# @Author : len
# @Email : ysling129@126.com
# @File : demo
# @Project : baidu_AI_API
# @description :


# import requests
#
# # client_id 为官网获取的AK， client_secret 为官网获取的SK
# host = 'https://aip.baidubce.com/oauth/2.0/token?grant_type=client_credentials&client_id=yftTogZQRmYB0dmBs35aCNiW&client_secret=pvgzdHr4UCuRoYGEMfmhOHlU6Ecl38Sq'
# response = requests.get(host)
# if response:
#     print(response.json())



import requests

url = "https://aip.baidubce.com/rpc/2.0/creation/v1/poem"

querystring = {"access_token":"24.ace5edb3c6eed186f98042739462bd45.2592000.1576118250.282335-17750875"}

payload = {
"text": "人才队伍",
"index": 0
}
headers = {
    'Content-Type': "application/json",
    'User-Agent': "PostmanRuntime/7.19.0",
    'Accept': "*/*",
    'Cache-Control': "no-cache",
    'Postman-Token': "09b53d13-d1d6-4dd7-8668-4739a587ee50,9835e566-a513-4042-90da-9f24f64f9864",
    'Host': "aip.baidubce.com",
    'Accept-Encoding': "gzip, deflate",
    'Content-Length': "42",
    'Connection': "keep-alive",
    'cache-control': "no-cache"
    }

response = requests.request("POST", url, data=payload, headers=headers, params=querystring)

print(response.text)