import json
import requests
import pandas  as pd
import numpy as np
from pandas.core.frame import DataFrame
from cmath import nan
import re
import time
import shutil
import os
from A000_dic_tache_attahement_chemin import *

path_stokage = r'C:/Users/tpeng/ENOE ENERGIE/Dataroom - Documents'

# ===================================================================
# get data depuis etape precendent
# ===================================================================
nomfile = r'res_003_Projet_attchement.txt'
with open(nomfile, "r",encoding='utf-8') as f:  # 打开文件
    str_json_df_projet_lien= f.read()  # 读取文件
json_df_projet_lien = json.loads(str_json_df_projet_lien)
df_projet_lien = pd.read_json(json_df_projet_lien,encoding="utf-8", orient='records')
# ===================================================================
# get data depuis etape precendent
# ===================================================================



# ===================================================================
# get data depuis etape precendent
# ===================================================================
nom_SPV = []
nom_SPV.append("AHAUSUN")
nom_SPV.append("ECO APEX 18")
nom_SPV.append("ECO-PV")
nom_SPV.append("EGPA")
nom_SPV.append("ECO-18")
nom_SPV.append("ENOE PV01")
nom_SPV.append("ENOE PV03")
nom_SPV.append("ENOE PV07")
nom_SPV.append("ENOE PV11")
nom_SPV.append("ENOE PV13")
nom_SPV.append("ENOE PV")
nom_SPV.append("EPSC")
nom_SPV.append("RONCE ENERGIE")
nom_SPV.append("SPV1")
nom_SPV.append("TENAO 3")
nom_SPV.append("TS014GRUI")
nom_SPV.append("TS031BIAR")
nom_SPV.append("TS015BIAR")
nom_SPV.append("TS007BOUE")
nom_SPV.append("ENOE PV02")
nom_SPV.append("BEENR-1")
nom_SPV.append("OUEST ENERGIES 1")
nom_SPV.append("ENOE PV14")
nom_SPV.append("ENOE PV15")
nom_SPV.append("ENOE PV17")




list_telecharge = []
index_telechargement = 0
for key_projet,row_projet in df_projet_lien.iterrows(): # parcourir les projet
    if row_projet["SPV"] in nom_SPV:
        for lienNom in dic_lien_chemin.keys():
            # if row_projet['nom projet']  in ["Lamé","Liengme"]:
            if True:
                if not pd.isna(row_projet[lienNom]):
                    nomPDF_wrik = row_projet["Nom_" + lienNom[4:]]
                    cheminPDF, nomPDF = dic_lien_chemin[lienNom]
                    thisPath = path_stokage + r"/" + row_projet["SPV"] +r"/Projets" + r"/" + row_projet['nom projet'] 
                    thisPath = thisPath +  cheminPDF +  r"/" + row_projet['nom projet'] +"-"+nomPDF +row_projet[lienNom][-4:]
                    createTime = time.mktime(time.strptime(row_projet["createdDate_" + lienNom[4:]], '%Y-%m-%dT%H:%M:%SZ'))
                    list_telecharge.append([thisPath,row_projet[lienNom],createTime,index_telechargement,nomPDF_wrik])
                    index_telechargement +=1

df_list_telecharge = pd.DataFrame(columns=["thisPath","lienNom","createTime","index_telechargement","nomPDF_wrik"],data=list_telecharge)
with open("res_004_df_list_telecharge.csv","w",encoding="utf-8") as f:
    f.write(df_list_telecharge.to_csv(index=None,line_terminator="\n"))



import os
import requests
from time import time
from multiprocessing.pool import ThreadPool
import threading
from multiprocessing import Pool, Lock
from multiprocessing import  Manager
import datetime

def url_response(url,mylist):
    global compt
    global compt_old
    global compt_new
    myPath, lien,creatTime,index_telecharge,nomPDF_wrik = url

    # print(myPath)
    if os.path.exists(myPath):
    # if True:
        myEtat = "exist"
        create_time_courrent = os.path.getmtime(myPath)
        if create_time_courrent != creatTime:
            myEtat = "exist_mis_a_jour"
    else:
        myEtat = "non_exist"

    if myEtat in ["exist_mis_a_jour", "non_exist"]:
        os.makedirs(os.path.dirname(myPath), exist_ok=True)
        r = requests.get(lien, stream=True)
        with open(myPath, 'wb') as f:
            for ch in r:
                f.write(ch)
        os.utime(myPath, (creatTime, creatTime))

    lock.acquire()
    if myEtat == "exist":
        mylist[1] +=1
    elif myEtat in ["non_exist","exist_mis_a_jour"]:
        mylist[2] +=1
    mylist[0] += 1
    if mylist[0] % 1 == 0:
        print("index_telecharge ",index_telecharge ,"order : ",mylist[0],"old : ",mylist[1],"new :",mylist[2],"all :",len(list_telecharge), \
            "nomPDF_wrik  ",nomPDF_wrik)
    # if myEtat in ["non_exist"]:
    #     print(      datetime.datetime.fromtimestamp(creatTime),
    #                 myPath
    #                 )
    #     print(myEtat,"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
    if myEtat in ["exist_mis_a_jour"]:
        print(datetime.datetime.fromtimestamp(create_time_courrent),"  ** ",
                    datetime.datetime.fromtimestamp(creatTime),
                    myPath
                    )
        print(myEtat,"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
    lock.release()

start = time()
# for x in list_telecharge:
#     url_response(x)
# ThreadPool(9).imap_unordered(url_response, list_telecharge)

def init(l):
	global lock
	lock = l

if __name__=="__main__":
    manager = Manager()
    mylist=Manager().list([0,0,0])
    lock = Lock()
    p=Pool(8,initializer=init, initargs=(lock,))
    for i in list_telecharge:
        p.apply_async(url_response,args=(i,mylist,))
    p.close()
    p.join()
    print("all process are over")

print(f"Time to download: {time() - start}")



for one_SPV in nom_SPV:
    vide_dossier = path_stokage + r"/" + one_SPV + r"/" +"Corporate"
    if not os.path.exists(vide_dossier):
        os.makedirs(vide_dossier) #多层创建目录