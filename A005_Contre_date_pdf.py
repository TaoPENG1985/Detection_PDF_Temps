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
from P000_dic_tache_attahement_chemin import *
from datetime import datetime
import datetime as dt

# ===================================================================
# get data depuis etape precendent
# ===================================================================
nomfile = r'Resultat\res_004_Projet_attchement.txt'
with open(nomfile, "r",encoding='utf-8') as f:  # 打开文件
    str_json_df_projet_lien= f.read()  # 读取文件
json_df_projet_lien = json.loads(str_json_df_projet_lien)
df_projet_lien = pd.read_json(json_df_projet_lien,encoding="utf-8", orient='records')
df_projet_lien = df_projet_lien.replace("",nan)

# ===================================================================
# les "tache_", "fichier_" pour afficher dans power BI
# ===================================================================
for tache_resume in dic_nomTacheV_cleAttchementV.keys():
    for i in ["tache_", "fichier_"]:
        df_projet_lien[i+tache_resume] = nan



# ===================================================================
# control: date de fin chantier <-> pdf - > date
# ===================================================================
for key_projet,row_projet in df_projet_lien.iterrows(): # parcourir les projet
    list_champ_ayantValeur = []
    list_date_ayantValeur =["date_debut_Chantier","date_Accord","date_PMBAIL"]
    # de date - > champs
    if not pd.isna(row_projet["date_fin_Chantier"]):
        list_champ_ayantValeur +=["PMBAIL"]
        if "PC" == row_projet["Type (AU)"]:
            list_champ_ayantValeur += ["DOC","DAACT"]
            list_champ_ayantValeur += ["CONSTATPC","CNRPC"]
            list_champ_ayantValeur += ["PRODPC","ARRETEPC"]
        elif "DP" == row_projet["Type (AU)"] :
            list_champ_ayantValeur += ["DAACT"]
            list_champ_ayantValeur += ["CONSTATDP","CNRDP"]
            list_champ_ayantValeur += ["PRODDP","ARRETEDP"]
        elif "PC+DP" == row_projet["Type (AU)"]:
            list_champ_ayantValeur += ["DOC","DAACT"]
            list_champ_ayantValeur += ["CONSTATPC","CNRPC"] +  ["CONSTATDP","CNRDP"]
            list_champ_ayantValeur += ["PRODPC","ARRETEPC"] + ["PRODDP","ARRETEDP"]

        for i in list_champ_ayantValeur:
            if row_projet["NB_"+i] == 0:
                df_projet_lien.loc[key_projet,"tache_" +i] = "0 ❌"
            if row_projet["conf_"+i] == 0:
                df_projet_lien.loc[key_projet,"fichier_" +i] = "0 ❌"
    # date - > date
        for j in list_date_ayantValeur:
            if pd.isna(row_projet[j]):
                df_projet_lien.loc[key_projet,j] = "❌ fin_Chantier"

    # pdf -> date
    if "PC" == row_projet["Type (AU)"] or "PC+DP" == row_projet["Type (AU)"]:
        if row_projet["NB_"+"DAACT"] > 0 and  row_projet["NB_"+"DOC"] > 0:
            if pd.isna(row_projet["date_fin_Chantier"]):
                df_projet_lien.loc[key_projet,"date_fin_Chantier"] = "❌ fichiers"
    if "DP" == row_projet["Type (AU)"] :
        if row_projet["NB_"+"DAACT"] >0:
            if pd.isna(row_projet["date_fin_Chantier"]):
                df_projet_lien.loc[key_projet,"date_fin_Chantier"] = "❌ fichiers"

# ===================================================================
# date debut chantier -> pdf ->date
# ===================================================================
list_champ_ayantValeur = ["DOC"]
list_date_ayantValeur =["date_Accord","date_PMBAIL"]
for key_projet,row_projet in df_projet_lien.iterrows(): # parcourir les projet
    if not pd.isna(row_projet["date_debut_Chantier"]):
    # date - > pdf
        for i in list_champ_ayantValeur:
            if row_projet["NB_"+i] == 0:
                df_projet_lien.loc[key_projet,"tache_" +i] = "0 ❌"
            if row_projet["conf_"+i] == 0:
                df_projet_lien.loc[key_projet,"fichier_" +i] = "0 ❌"
    # date - > date
        for j in list_date_ayantValeur:
            if pd.isna(row_projet[j]):
                df_projet_lien.loc[key_projet,j] = "❌ debut_Chantier"   


# ===================================================================
# control depuis Accord (avant date Accord) double sens
# ===================================================================
for key_projet,row_projet in df_projet_lien.iterrows(): # parcourir les projet
    list_champ_ayantValeur = []
    list_date_ayantValeur =["date_PMBAIL"]

   # ==================================
    # de date - > champs
    # ===================================
    if not pd.isna(row_projet["date_Accord"]):

        list_champ_ayantValeur +=["PMBAIL"]
        if "PC" == row_projet["Type (AU)"]:
            list_champ_ayantValeur += ["PRODPC","ARRETEPC"]
        elif "DP" == row_projet["Type (AU)"] :
            list_champ_ayantValeur += ["PRODDP","ARRETEDP"]
        elif "PC+DP" == row_projet["Type (AU)"]:
            list_champ_ayantValeur += ["PRODPC","ARRETEPC"] + ["PRODDP","ARRETEDP"]
        
        for i in list_champ_ayantValeur:
            if row_projet["NB_"+i] == 0:
                df_projet_lien.loc[key_projet,"tache_" +i] = "0 ❌"
            if row_projet["conf_"+i] == 0:
                df_projet_lien.loc[key_projet,"fichier_" +i] = "0 ❌"
    # ==================================
    # de date - > date
    # ===================================
        
        for j in list_date_ayantValeur:
            if pd.isna(row_projet[j]):
                df_projet_lien.loc[key_projet,j] = "❌ Accord"   
        
    # ==================================
    # de champs a date
    # ===================================     
    if "PC" == row_projet["Type (AU)"]:
        if row_projet["NB_"+"PRODPC"] > 0 and  row_projet["NB_"+"ARRETEPC"] > 0:
            if pd.isna(row_projet["date_Accord"]):
                df_projet_lien.loc[key_projet,"date_Accord"] = "❌ selon fichiers"    

    if "DP" == row_projet["Type (AU)"] :
        if row_projet["NB_"+"PRODDP"] > 0 and row_projet["NB_"+"ARRETEDP"] > 0:
            if pd.isna(row_projet["date_Accord"]):
                df_projet_lien.loc[key_projet,"date_Accord"] = "❌ selon fichiers"
    
    if "PC+DP" == row_projet["Type (AU)"]:
        if row_projet["NB_"+"PRODPC"] > 0 and  row_projet["NB_"+"ARRETEPC"] > 0:
            if row_projet["NB_"+"PRODDP"] > 0 and row_projet["NB_"+"ARRETEDP"] > 0:
                if pd.isna(row_projet["date_Accord"]):
                    df_projet_lien.loc[key_projet,"date_Accord"] = "❌ selon fichiers"

# ===================================================================
# control depuis Accord (6 mois apres date Accord)  sens unique
# ===================================================================
for key_projet,row_projet in df_projet_lien.iterrows(): # parcourir les projet
    list_champ_ayantValeur = []
   # ==================================
    # de date - > champs
    # ===================================
    if not pd.isna(row_projet["date_Accord"]):
        
        list_champ_ayantValeur = []

        if not("❌" in row_projet["date_Accord"]) and len(row_projet["date_Accord"]) > 1:
            timeStamp = datetime.strptime(row_projet["date_Accord"], "%Y-%m-%d")
            timeStamp_dans6mois = timeStamp + dt.timedelta(days = 180)
            new_Today = datetime.now()
            if timeStamp_dans6mois < new_Today:
                df_projet_lien.loc[key_projet,"+6mois"] = timeStamp_dans6mois.strftime('%Y-%m-%d')
                if "PC" == row_projet["Type (AU)"]:
                    list_champ_ayantValeur += ["CONSTATPC","CNRPC"]
                elif "DP" == row_projet["Type (AU)"] :
                    list_champ_ayantValeur += ["CONSTATDP","CNRDP"]
                elif "PC+DP" == row_projet["Type (AU)"]:
                    list_champ_ayantValeur += ["CONSTATPC","CNRPC"] +  ["CONSTATDP","CNRDP"]

        for i in list_champ_ayantValeur:
            if row_projet["NB_"+i] == 0:
                df_projet_lien.loc[key_projet,"tache_" +i] = "0 ❌"
            if row_projet["conf_"+i] == 0:
                df_projet_lien.loc[key_projet,"fichier_" +i] = "0 ❌"


# ===================================================================
# control depuis date PMbail, double sens
# ===================================================================
for key_projet,row_projet in df_projet_lien.iterrows(): # parcourir les projet
    # ===================================================================
    # de temps a date
    # ===================================================================    
    if not pd.isna(row_projet["date_PMBAIL"]):
        for i in ["PMBAIL"]:
            if row_projet["NB_"+i] == 0:
                df_projet_lien.loc[key_projet,"tache_" +i] = "0 ❌"
            if row_projet["conf_"+i] == 0:
                df_projet_lien.loc[key_projet,"fichier_" +i] = "0 ❌"
    # ===================================================================
    # de  date a temps
    # ===================================================================        
    if row_projet["conf_PMBAIL"] > 0:
        if pd.isna(row_projet["date_PMBAIL"]):
            df_projet_lien.loc[key_projet,"date_PMBAIL"] = "❌ selon fichiers"

# ===================================================================
# control depuis date Bail , n s'aigit pas de atutre groupes , double sens
# ===================================================================
for key_projet,row_projet in df_projet_lien.iterrows(): # parcourir les projet
    # ===================================================================
    # de date a champs
    # ===================================================================    
    if not pd.isna(row_projet["date_BAIL"]):
        for i in ["SBN"]:
            if row_projet["NB_"+i] == 0:
                df_projet_lien.loc[key_projet,"tache_" +i] = "0 ❌"
            if row_projet["conf_"+i] == 0:
                df_projet_lien.loc[key_projet,"fichier_" +i] = "0 ❌"
    # ===================================================================
    # de  champs a date
    # ===================================================================        
    if row_projet["conf_SBN"] >0:
        if pd.isna(row_projet["date_BAIL"]):
            df_projet_lien.loc[key_projet,"date_BAIL"] = "❌ selon fichiers"

    # ===================================================================
    # si Bail n'est pas vide, l'eurrer dans date PMBAIL est supprimée 
    # =================================================================== 
    if not pd.isna(row_projet["date_BAIL"]):
        if not "❌" in row_projet["date_BAIL"]:
            if not pd.isna(row_projet["date_PMBAIL"]):      
                if  "❌" in row_projet["date_PMBAIL"]:
                    df_projet_lien.loc[key_projet,"date_PMBAIL"] = "Exempt_Bail"

# ===================================================================
# control prorog 1pc 2pc
# ===================================================================
list_prorog_1PC = {"PROROG1PC","PROROGCONSTAT1PC","PROROGCNR1PC"}
list_prorog_2PC = {"PROROG2PC","PROROGCONSTAT2PC","PROROGCNR2PC"}

for key_projet,row_projet in df_projet_lien.iterrows(): # parcourir les projet

    # ===================================================================
    #  2PC => 1PC, 
    # ===================================================================
    flag_2PC = False
    for i in list_prorog_2PC:
        if row_projet["NB_"+i] > 0:
            flag_2PC = True

    if flag_2PC:
        for i in list_prorog_1PC:
            if row_projet["NB_"+i] == 0:
                df_projet_lien.loc[key_projet,"tache_" +i] = "0 ❌"
            if row_projet["conf_"+i] == 0:
                df_projet_lien.loc[key_projet,"fichier_" +i] = "0 ❌"
    # ===================================================================
    #   1PC => PRODPC,ARRETEPC
    # ===================================================================
    flag_1PC = flag_2PC
    for i in list_prorog_1PC:
        if row_projet["NB_"+i] > 0:
            flag_1PC = True

    if flag_2PC:
        for i in ["PRODPC","ARRETEPC"]:
                if row_projet["NB_"+i] == 0:
                    df_projet_lien.loc[key_projet,"tache_" +i] = "0 ❌"
                if row_projet["conf_"+i] == 0:
                    df_projet_lien.loc[key_projet,"fichier_" +i] = "0 ❌"
    # ===================================================================
    #  FLAG 1_2_pc
    # ===================================================================
    df_projet_lien.loc[key_projet,"flag_1_2_pc"] = str(flag_1PC)

# ===================================================================
# control prorog 1dp 2dp
# ===================================================================
list_prorog_1DP = {"PROROG1DP","PROROGCONSTAT1DP","PROROGCNR1DP"}
list_prorog_2DP = {"PROROG2DP","PROROGCONSTAT2DP","PROROGCNR2DP"}

for key_projet,row_projet in df_projet_lien.iterrows(): # parcourir les projet

    # ===================================================================
    #  2DP => 1DP, 
    # ===================================================================
    flag_2DP = False
    for i in list_prorog_2DP:
        if row_projet["NB_"+i] > 0:
            flag_2DP = True

    if flag_2DP:
        for i in list_prorog_1DP:
            if row_projet["NB_"+i] == 0:
                df_projet_lien.loc[key_projet,"tache_" +i] = "0 ❌"
            if row_projet["conf_"+i] == 0:
                df_projet_lien.loc[key_projet,"fichier_" +i] = "0 ❌"
    # ===================================================================
    #   1DP => PRODP,ARRETEDP
    # ===================================================================
    flag_1DP = flag_2DP
    for i in list_prorog_1DP:
        if row_projet["NB_"+i] > 0:
            flag_1DP = True

    if flag_1DP:
        for i in ["PRODDP","ARRETEDP"]:
                if row_projet["NB_"+i] == 0:
                    df_projet_lien.loc[key_projet,"tache_" +i] = "0 ❌"
                if row_projet["conf_"+i] == 0:
                    df_projet_lien.loc[key_projet,"fichier_" +i] = "0 ❌"
    # ===================================================================
    #  FLAG 1_2_dp
    # ===================================================================
    df_projet_lien.loc[key_projet,"flag_1_2_dp"] = str(flag_1DP)


# ===================================================================
# control prorog PCM1, PCM2, PCM3,TANS
# ===================================================================
dic_PPPT = {    "PCM1" : ["PCM","PCMARRETE","PCMCONSTAT","PCMCNR"],
                "PCM2" : ["PCM2","PCMARRETE2","PCMCONSTAT2","PCMCNR2"],
                "PCM3" : ["PCM3","PCMARRETE3","PCMCONSTAT3","PCMCNR3"],
                "TANS" : ["TRANSFERT","TRANSFERTCONSTAT","TRANSFERTCNR"]
}

for key_projet,row_projet in df_projet_lien.iterrows(): # parcourir les projet
    for key ,value in dic_PPPT.items():
        if row_projet["NB_"+value[-1]] > 0:
            for resume in value:
                if row_projet["NB_"+resume] == 0:
                    df_projet_lien.loc[key_projet,"tache_"+resume] == "0 ❌"
                if row_projet["conf_"+resume] == 0:
                    df_projet_lien.loc[key_projet,"fichier_"+resume] == "0 ❌"
        
        for resume in value: 
            if row_projet["NB_"+resume] > 0:
                df_projet_lien.loc[key_projet,"flag_"+key] = "True"
# ===================================================================
# pour tous  1 
# ===================================================================
for key_projet,row_projet in df_projet_lien.iterrows(): # parcourir les projet
    for tache_resume in dic_nomTacheV_cleAttchementV.keys():
        if row_projet["conf_"+tache_resume] == 0 and row_projet["fichier_"+ tache_resume] != "0 ❌":
            df_projet_lien.loc[key_projet,"fichier_"+tache_resume] = nan
        elif row_projet["conf_"+tache_resume] == 1:
            df_projet_lien.loc[key_projet,"fichier_"+tache_resume] = "1 🔗"
        elif row_projet["conf_"+tache_resume] > 1:
            df_projet_lien.loc[key_projet,"fichier_"+tache_resume] =  str(row_projet["conf_"+tache_resume]) +" ⚠️"

        if row_projet["NB_"+tache_resume] == 0 and row_projet["tache_"+tache_resume] != "0 ❌": 
            df_projet_lien.loc[key_projet,"tache_"+tache_resume] = nan
        elif row_projet["NB_"+tache_resume] == 1 :
            df_projet_lien.loc[key_projet,"tache_"+tache_resume] = "1 ✅"
        elif row_projet["NB_"+tache_resume] > 1:
            df_projet_lien.loc[key_projet,"tache_"+tache_resume] =  str(row_projet["NB_"+tache_resume]) +" ⚠️"
# ===================================================================
# par tous 2, CS DP RR 
# ===================================================================
list_tache_resume_AP = "PRODPC,ARRETEPC,CONSTATPC,CNRPC," + \
                    "PRODDP,ARRETEDP,CONSTATDP,CNRDP," + \
                    "PROROG1PC,PROROGCONSTAT1PC,PROROGCNR1PC," +\
                    "PROROG2PC,PROROGCONSTAT2PC,PROROGCNR2PC," +\
                    "PROROG1DP,PROROGCONSTAT1DP,PROROGCNR1DP," +\
                    "PROROG2DP,PROROGCONSTAT2DP,PROROGCNR2DP," +\
                    "DOC,DAACT," + \
                    "PCM,PCMARRETE,PCMCONSTAT,PCMCNR," + \
                    "PCM2,PCMARRETE2,PCMCONSTAT2,PCMCNR2," + \
                    "PCM3,PCMARRETE3,PCMCONSTAT3,PCMCNR3," + \
                    "TRANSFERT,TRANSFERTCONSTAT,TRANSFERTCNR," + \
                    "LAUREAT,DOSSIERAO"


for key_projet,row_projet in df_projet_lien.iterrows():
    flag_type_projet = "normal"
    if (row_projet["nom projet"][-3:] in ["-AP","-RR","-CS"]):
        flag_type_projet = row_projet["nom projet"][-3:]
    if "-AP-" in row_projet["nom projet"]:
        flag_type_projet = "-AP"
    if "-RR-" in row_projet["nom projet"]:
        flag_type_projet = "-RR"
    if "-CS-" in row_projet["nom projet"]:
        flag_type_projet = "-CS"

    if flag_type_projet != "normal":
        for tache_resume in list_tache_resume_AP.split(","):
            if row_projet["NB_"+tache_resume] == 0:
                df_projet_lien.loc[key_projet,"tache_"+tache_resume] = nan
            else:
                df_projet_lien.loc[key_projet,"tache_"+tache_resume] =str(row_projet["NB_"+tache_resume]) + " ⚠️"
            if row_projet["conf_"+tache_resume] == 0:
                df_projet_lien.loc[key_projet,"fichier_"+tache_resume] = nan
            else:
                df_projet_lien.loc[key_projet,"fichier_"+tache_resume] =str(row_projet["conf_"+tache_resume]) + " ⚠️"
        df_projet_lien.loc[key_projet,"projet type"] = flag_type_projet
    else:
        df_projet_lien.loc[key_projet,"projet type"] = flag_type_projet

# ===================================================================
# par tous 3, detect new 
# ===================================================================
for key_projet,row_projet in df_projet_lien.iterrows(): # parcourir les projet
    for tache_resume in dic_nomTacheV_cleAttchementV.keys():
        if not (pd.isna(row_projet["createdDate_"+tache_resume])):
            if datetime.strptime(row_projet["createdDate_"+tache_resume], "%Y-%m-%dT%H:%M:%SZ") > datetime.now() - dt.timedelta(days = 7):
                if df_projet_lien.loc[key_projet,"conf_"+tache_resume] > 0:
                    df_projet_lien.loc[key_projet,"fichier_"+tache_resume] += "📩"



    # df_res["createdDate_"+key] = nan

# ===================================================================
# change format du temp ttmmdd -> dd mm tt
# ===================================================================
list_date_champs =["date_Accord","+6mois","date_PMBAIL","date_fin_Chantier",\
                     "date_BAIL","date_debut_Chantier","date accord (PCM1)",
                     "date accord (PCM2)","date accord (PCM3)","date accord (Transf PC)",\
                        "date accord (Transf DP)","Validite"]
for key_projet,row_projet in df_projet_lien.iterrows(): # parcourir les projet
    for key_date in list_date_champs: # parcourir les combinaison
        if not (pd.isna(row_projet[key_date]) or   "❌" in (row_projet[key_date])): 
            if len(row_projet[key_date]) == 10:
                str_temp = row_projet[key_date]
                df_projet_lien.loc[key_projet,key_date] = str_temp[8:10] + str_temp[4:8] +str_temp[:4]



with open(r"C:\Users\tpeng\ENOE ENERGIE\SI - Tao PENG\Projet_detectPDF0032\res_003_Projet_date_attchement.csv","w",encoding="utf-8") as f:
    f.write(df_projet_lien.to_csv(index=None,line_terminator="\n"))
    f.write

df_tete_tableau = DataFrame()
index = 0

for i in ["tache_PMBAIL","fichier_PMBAIL","date_PMBAIL"]:
    df_tete_tableau.loc[index,"nom_tete"] = "PMBAIL"
    df_tete_tableau.loc[index,"nom_champs"] = i
    index +=1

with open(r"C:\Users\tpeng\ENOE ENERGIE\SI - Tao PENG\Projet_detectPDF0032\double_tete_PMBAIL.csv","w",encoding="utf-8") as f:
    f.write(df_tete_tableau.to_csv(index=None,line_terminator="\n"))
    f.write