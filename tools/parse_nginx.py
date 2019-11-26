import pandas as pd
import json
import re
import chardet

with open('/Users/tiansc/downloaded_data_20190924.txt', 'rb') as file:
    lines = file.readlines()
#     print(len(lines))
    for line in lines[:1]:
        try:
            line = json.loads(line)
            item = line['content']
        except Exception as e:
            pass
#             print(1, e, line)
        else:

            item1 = bytes(item.encode('ascii','ignore')).decode('unicode_escape')
            print(item1) 
            ip = re.findall(r"\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b", item1)
            pattern = re.compile(r'name=(.*?)--', re.S)   # 查找数字
            dic_x = {}
            dic_y = item1.split('\n')[-1].split(' ')[-1].strip('"')

            rets = pattern.findall(item1)
            for ret in rets[:]:

                ret = re.sub(r'(Content-Length: \d+)', '', ret)
                try:
                    ret_lst = [ x.strip().strip('"') for x in ret.split('\n') if x.strip() ]
                    if len(ret_lst)==2:
                        dic_x[ret_lst[0]] = ret_lst[1]
                    elif len(ret_lst)==1:
                        dic_x[ret_lst[0]] = ''
                    else:
                        pass
                except Exception as e:
                    print(2, e, rets)
                    break
            try:
                dic_y = eval(dic_y)['data']  
            except:
                pass
            else:
                dic_z = {**dic_x, **dic_y}
                if len(dic_z.keys())<5:
                    pass
                else:
                    try:
                        print(dic_z['transNo']+','+dic_z['prescriptionId']+','+dic_z['sys_d']+','+line['time']+','+ip[0]+','+ip[1])
                    except:
                        pass
        