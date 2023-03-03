import json
import requests
import pandas  as pd
import numpy as np
from pandas.core.frame import DataFrame
from cmath import nan

# ===================================================================
# Les parametres
# ===================================================================
myToken = "eyJ0dCI6InAiLCJhbGciOiJIUzI1NiIsInR2IjoiMSJ9.eyJkIjoie1wiYVwiOjI3MjIxMjUsXCJpXCI6ODM5MzMyMixcImNcIjo0NjM1NDQxLFwidVwiOjE0ODk5ODQzLFwiclwiOlwiVVNcIixcInNcIjpbXCJXXCIsXCJGXCIsXCJJXCIsXCJVXCIsXCJLXCIsXCJDXCIsXCJEXCIsXCJNXCIsXCJBXCIsXCJMXCIsXCJQXCJdLFwielwiOltdLFwidFwiOjB9IiwiaWF0IjoxNjY0NzgzNzg3fQ.bJxVIxGToNC5RLHamvi8Ckj--Ug2-LSp2Kf0d9DS8Nc"
url = 'https://www.wrike.com/api/v4'

# ===================================================================
# recuprer les taches dans 0032
# ===================================================================
df_res = DataFrame()
id_projet_0032 = "IEACTCKNI4LT5UCB"
nextPageToken = ''
index = 0
while(nextPageToken != 'nonTrouve'):
    # ===================================================================
    # lancer un API
    # ===================================================================
    reponse = requests.get(url + '/folders/' + id_projet_0032 + '/tasks', 
                            headers =   {  'Authorization':  'bearer ' + myToken },
                            params =    {  "descendants": "true",
                                            "subTasks": "true",
                                            'pageSize':'1000',
                                            'fields':'["hasAttachments","parentIds"]',
                                            'nextPageToken':nextPageToken})
    # with open(r"Observation\tasksDans0032.txt","w",encoding="utf-8") as f:
    #         f.write(reponse.text)
    # ===================================================================
    # recuperer les valeurs
    # ===================================================================
    for courrent_tache in reponse.json()['data']:
        df_res.loc[index,"id_parent"] = courrent_tache['id']
        df_res.loc[index,"nom_parent"] = courrent_tache['title']
        df_res.loc[index,"permalink"] = courrent_tache['permalink']
        index +=1
    
    # ===================================================================
    # recuperer le url/api pour page suivant, si non, c'est la dernier API 
    # ===================================================================
    try:
        nextPageToken = reponse.json()['nextPageToken']
    except:
        nextPageToken = 'nonTrouve'

    print(nextPageToken,"    ",index) # afficher les index

# =====================================================
# sauvgarder
# ====================================================
with open(r"Resultat\res_003_id_nom_Tahce.csv","w",encoding="utf-8") as f:
    f.write(df_res.to_csv(index=None,line_terminator="\n"))

with open(r"Resultat\res_003_id_nom_Tahce.txt","w",encoding="utf-8") as f:
    f.write(json.dumps( df_res.to_json(),ensure_ascii=False))