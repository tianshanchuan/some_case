import json
import os
import csv

path = r'/Users/tiansc/Downloads/20小升初社群/'
out_path = r'/Users/tiansc/Downloads/20小升初社群/20小升初社群_ret.csv'

goal_json_files = []

g = os.walk(path) 
for path,dir_list,file_list in g:  
    for file_name in file_list:  
        goal_json_files.append([os.path.join(path, file_name),file_name.rstrip('.json')])

goal_json_files = [ x for x in goal_json_files if x[0].endswith('.json')]

# print(goal_json_files)

with open(out_path, 'w+', encoding='utf-8') as j:
    writer = csv.writer(j)
    for file_path in goal_json_files[:]:
        with open(file_path[0],'rb') as f:
            ret = json.loads(f.read())
            for item in ret:
                if item['nick_name']:
                    nick_name = item['nick_name']
                else:
                    nick_name = None
                try:
                    remark_name = item['remark_name']
                except:
                    remark_name = None
                wxid = item['wxid']
                user_name = item['user_name']
                kv_lst = [file_path[1],nick_name,wxid, user_name,remark_name]
                print(kv_lst)
                writer.writerows([kv_lst])
#                 print(file_path[1],nick_name, remark_name)