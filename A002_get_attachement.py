import json
import requests
import pandas  as pd
import numpy as np
from pandas.core.frame import DataFrame
from cmath import nan
from datetime import datetime, timedelta

import time

myToken = "eyJ0dCI6InAiLCJhbGciOiJIUzI1NiIsInR2IjoiMSJ9.eyJkIjoie1wiYVwiOjI3MjIxMjUsXCJpXCI6ODM5MzMyMixcImNcIjo0NjM1NDQxLFwidVwiOjE0ODk5ODQzLFwiclwiOlwiVVNcIixcInNcIjpbXCJXXCIsXCJGXCIsXCJJXCIsXCJVXCIsXCJLXCIsXCJDXCIsXCJEXCIsXCJNXCIsXCJBXCIsXCJMXCIsXCJQXCJdLFwielwiOltdLFwidFwiOjB9IiwiaWF0IjoxNjY0NzgzNzg3fQ.bJxVIxGToNC5RLHamvi8Ckj--Ug2-LSp2Kf0d9DS8Nc"
url = 'https://www.wrike.com/api/v4'
id_robot = 'KUAKER7X'

# ===================================================================
# Recuperer les meta data sur les attchements de Wrike
# une requete retourne au plus les data de  30 jour plus recent, donc
#  il faut que filter par temps (createdDate)  pour recouperer tous les 
# attchements apres une date (date_plusAcien)
# ===================================================================
mydelta = timedelta(days=29)
endTime = datetime.now()
startime = endTime - mydelta
list_temps = []
# print(endTime.strftime("%Y-%m-%dT%H:%M:%SZ"))
date_plusAcien =  datetime.strptime('2018-10-05 00:00:00', '%Y-%m-%d %H:%M:%S')
# print(date_plusAcien)

for i in range(1,1000):
    strEndTime = endTime.strftime("%Y-%m-%dT%H:%M:%SZ")
    strStarTime = startime.strftime("%Y-%m-%dT%H:%M:%SZ")
    list_temps.append(' {"end":"'  +strEndTime + '", "start": "' +strStarTime +  '"}'  )
    endTime = startime
    startime = startime - mydelta
    if startime < date_plusAcien:
        break
print(list_temps)


# =====================================================
# API
# ====================================================
df_res = DataFrame()

index = 0
for temps in list_temps:
    reponse = requests.get(url + '/attachments', 
                headers = {'Authorization':  'bearer ' + myToken },
                                 params = {'createdDate': temps,
                                            "withUrls": "true"})
    with open(r"Observation\all_attchements" +temps[9:16]+".txt","w",encoding="utf-8") as f:
        f.write(reponse.text)
    
    for courrent_attachement in reponse.json()['data']:
        if courrent_attachement['authorId'] != id_robot:
        # if True:
            try:
                df_res.loc[index,"id_parent"] = courrent_attachement['taskId']
                df_res.loc[index,"id_attachement"] = courrent_attachement['id']
                df_res.loc[index,"nom_attachement"] = courrent_attachement['name']
                df_res.loc[index,"url"] = courrent_attachement['url']
                df_res.loc[index,"createdDate"] = courrent_attachement['createdDate']

                if "Saint-Martin-de-Queyrières-B0201-PC-VF.pdf" in courrent_attachement['name']:
                    print("on trouve Saint-Martin-de-Queyrières-B0201-PC-VF.pdf")
                if "Saint-Martin" in courrent_attachement['name']:
                    print(courrent_attachement['name'])

            except:
                err = ''

        index +=1

df_res["Mis à jour"] =  time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())) 


writer = pd.ExcelWriter('res_001-attachementIDs.xlsx')
df_res.to_excel(writer,float_format='%.5f',index = False)
writer.save()

with open(r"Resultat\res_002_attachement.csv","w",encoding="utf-8") as f:
    f.write(df_res.to_csv(index=None,line_terminator="\n"))

with open(r"Resultat\res_002_attachement.txt","w",encoding="utf-8") as f:
    f.write(json.dumps( df_res.to_json(),ensure_ascii=False))
