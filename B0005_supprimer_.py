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
from A0003_1_dic_tache_attahement import *


# ===================================================================
# get all tabeau comme rapport PowerBI
# ===================================================================
nomfile = r'res_003_Projet_attchement.txt'
with open(nomfile, "r",encoding='utf-8') as f:  # 打开文件
    str_json_df_projet_lien= f.read()  # 读取文件
json_df_projet_lien = json.loads(str_json_df_projet_lien)
df_projet_lien = pd.read_json(json_df_projet_lien,encoding="utf-8", orient='records')


# ===================================================================
# get all tabeau comme rapport PowerBI
# ===================================================================
csv_file = "res_004_df_list_telecharge.csv"
csv_data = pd.read_csv(csv_file, low_memory = False)#防止弹出警告
df_list_telecharge = pd.DataFrame(csv_data)
list_patch = df_list_telecharge["thisPath"][:].to_list()
list_patch = [ x.replace("/","\\") for x in list_patch]


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


root = r'C:\Users\tpeng\ENOE ENERGIE\Dataroom - Documents'

pathnames = []
for mySPV in nom_SPV:
    path = os.path.join(root,mySPV,"Projets")
    for (dirpath, dirnames, filenames) in os.walk(path):
        for filename in filenames:
            pathnames += [os.path.join(dirpath, filename)]

list_suprrimer =[]
num = 0
for patch_file in pathnames[:]:
    if (patch_file in list_patch) or ("xxxxxxxxxxxxxxxx" in patch_file) :
        if os.path.getsize(patch_file) == 0:
            print( "vide ",  patch_file)
            list_suprrimer.append(patch_file)
            num+1 
    else:
        print("not ",patch_file)
        list_suprrimer.append(patch_file)
        num +=1
print(num)


for i in list_suprrimer:
    os.remove(i)

# ===================================================================
# SUPRIME des dissier vide
# ===================================================================

if True:
# if False:
    pathname_dossierVide = []
    for mySPV in nom_SPV:
        path = os.path.join(root,mySPV,"Projets")
        for (dirpath, dirnames, filenames) in os.walk(path):
            if dirnames == [] and filenames == []:
                print(dirpath)
                print(dirnames)
                print(filenames)
                print("XXXXXXXXXXXXXXXXXXXXXXX")
                os.rmdir(dirpath)
                pathname_dossierVide.append(dirpath)

print(len(pathname_dossierVide))
