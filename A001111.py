import json
import requests
import pandas  as pd
import numpy as np
from pandas.core.frame import DataFrame
from cmath import nan
import re
from datetime import datetime
from P000_dic_tache_attahement_chemin import *
# ===================================================================
# fonction qui get text, return df
# ===================================================================
def get_text_to_df(nomfile):
    with open(nomfile, "r",encoding='utf-8') as f:  # 打开文件
        str_json_df_lesProjet = f.read()  # 读取文件
    json_df_lesProjet = json.loads(str_json_df_lesProjet)
    return pd.read_json(json_df_lesProjet,encoding="utf-8", orient='records')
# ===================================================================
# recuperer projet dans 0032, attachements dans Wrike,  Tahce de 0032
# ===================================================================
df_projet = get_text_to_df(r'Resultat\res_001_Projet0032.txt')
df_attachement =  get_text_to_df( r'Resultat\res_002_attachement.txt')
df_Tahce = get_text_to_df( r'Resultat\res_003_id_nom_Tahce.txt')

# ===================================================================
# pre-Filtrez les tache: si une tache contient des mot cle, elle est valide
# ===================================================================
for key_tache,row_tache in df_Tahce.iterrows():
    for key_tacheV,cleTacheV in dic_nomTacheV_cleTacheV.items():
        if cleTacheV.upper() in row_tache["nom_parent"].upper():
            df_Tahce.loc[key_tache,"valide"] = 1
            break
df_Tahce = df_Tahce.drop(df_Tahce[(df_Tahce['valide'] != 1)].index) 
# observer 
with open("OB_003_attachement_1.csv","w",encoding="utf-8") as f:
    f.write(df_attachement.to_csv(index=None,line_terminator="\n"))

# ===================================================================
# pre-Filtrez les attachement: Si l'identifiant de leur père (tâche) est en 0032, 
# si oui , gardez-le; si non, le supprimer. 
# ===================================================================
for key_attachement,row_attachement in df_attachement.iterrows():
    index_parentID = df_Tahce[(df_Tahce.id_parent==row_attachement["id_parent"])].index.tolist()
    df_attachement.loc[key_attachement,"valide"] = len(index_parentID)
df_attachement = df_attachement.drop(df_attachement[(df_attachement['valide'] != 1)].index) 
df_attachement.reset_index(drop=True, inplace=True)
# observer 
with open("OB_003_attachement_1.csv","w",encoding="utf-8") as f:
    f.write(df_attachement.to_csv(index=None,line_terminator="\n"))

# ===================================================================
# les preparation avant la combination
# ===================================================================
p1 = re.compile(r'[[](.*?)[]]', re.S) # preparer un instance de 《re》 pour recuprer les text dans [] 

    # ===================================================================
    # get les nom des projet et son index dans df_projet
    # ===================================================================
list_projet = list(set(df_projet["nom projet"].to_list()))
index_projet =  list(range(len(list_projet)))

    # ===================================================================
    # les dinctionnaires pour info base
    # ===================================================================
dic_KeyNom_ValueIndex = dict(zip(list_projet, index_projet))
dic_KeyNom_ValueId = dict(zip(df_projet['nom projet'], df_projet['work_Id']))
dic_KeyNom_ValueSPV = dict(zip(df_projet['nom projet'], df_projet['*SPV']))
dic_KeyNom_ValuePermalink = dict(zip(df_projet['nom projet'], df_projet['permalink']))
dic_KeyNom_ValueTypeAu = dict(zip(df_projet['nom projet'], df_projet['Type (AU)']))
dic_KeyNom_ValueResponsable = dict(zip(df_projet['nom projet'], df_projet["Responsable Dev"]))

    # ===================================================================
    # les dinctionnaires pour info DatePMBAIL，DateBAIL，DateAccord，DateFinChantier，DateDebutChantier
    # ===================================================================
dic_KeyNom_ValueDatePMBAIL = dict(zip(df_projet['nom projet'], df_projet['📅 Signature PMBAIL']))
dic_KeyNom_ValueDateBAIL = dict(zip(df_projet['nom projet'], df_projet['📆 Signature Bail']))
dic_KeyNom_ValueDateAccord = dict(zip(df_projet['nom projet'], df_projet['Accord']))
dic_KeyNom_ValueDateFinChantier = dict(zip(df_projet['nom projet'], df_projet['Date (Fin Chantier)']))
dic_KeyNom_ValueDateDebutChantier = dict(zip(df_projet['nom projet'], df_projet['Date (Début Chantier)']))

    # ===================================================================
    # les dinctionnaires pour info DatePCM1，DatePCM3，DatePCM3
    # ===================================================================
dic_KeyNom_ValueDatePCM1 = dict(zip(df_projet['nom projet'], df_projet["Date accord (PCM1)"]))
dic_KeyNom_ValueDatePCM2 = dict(zip(df_projet['nom projet'], df_projet["Date accord (PCM2)"]))
dic_KeyNom_ValueDatePCM3 = dict(zip(df_projet['nom projet'], df_projet["Date accord (PCM3)"]))
    # ===================================================================
    # les dinctionnaires pour info DateTransPC，DateTransDP
    # ===================================================================
dic_KeyNom_ValueDateTransPC = dict(zip(df_projet['nom projet'], df_projet["Date accord (Transf PC)"]))
dic_KeyNom_ValueDateTransDP = dict(zip(df_projet['nom projet'], df_projet["📅  Date accord (Transf DP)"]))

dic_KeyNom_ValueDateValidite = dict(zip(df_projet['nom projet'], df_projet["Validité"]))
dic_KeyNom_ValueReceptionDAACT = dict(zip(df_projet['nom projet'], df_projet["Réception DAACT"]))


# ===================================================================
# Initialiser deux table vide
# ===================================================================
df_res = DataFrame() # 
df_exception = DataFrame() # 
index_df_exception = 0

# ===================================================================
# Pour tous les projet 
# ===================================================================
for i in index_projet:
    df_res.loc[i,"nom projet"] = list_projet[i]
    df_res.loc[i,"work_Id"] = dic_KeyNom_ValueId[list_projet[i]]
    df_res.loc[i,"SPV"] = dic_KeyNom_ValueSPV[list_projet[i]]
    df_res.loc[i,"permalink"] = dic_KeyNom_ValuePermalink[list_projet[i]]
    df_res.loc[i,"Type (AU)"] = dic_KeyNom_ValueTypeAu[list_projet[i]]
    df_res.loc[i,"Responsable"] = dic_KeyNom_ValueResponsable[list_projet[i]]

    df_res.loc[i,"date_PMBAIL"] = dic_KeyNom_ValueDatePMBAIL[list_projet[i]]
    df_res.loc[i,"date_BAIL"] = dic_KeyNom_ValueDateBAIL[list_projet[i]]
    df_res.loc[i,"date_Accord"] = dic_KeyNom_ValueDateAccord[list_projet[i]]
    df_res.loc[i,"date_fin_Chantier"] = dic_KeyNom_ValueDateFinChantier[list_projet[i]]
    df_res.loc[i,"date_debut_Chantier"] = dic_KeyNom_ValueDateDebutChantier[list_projet[i]]
    
    df_res.loc[i,"date accord (PCM1)"] = dic_KeyNom_ValueDatePCM1[list_projet[i]]
    df_res.loc[i,"date accord (PCM2)"] = dic_KeyNom_ValueDatePCM2[list_projet[i]]
    df_res.loc[i,"date accord (PCM3)"] = dic_KeyNom_ValueDatePCM3[list_projet[i]]

    df_res.loc[i,"date accord (Transf PC)"] = dic_KeyNom_ValueDateTransPC[list_projet[i]]
    df_res.loc[i,"date accord (Transf DP)"] = dic_KeyNom_ValueDateTransDP[list_projet[i]]

    df_res.loc[i,"Validite"] = dic_KeyNom_ValueDateValidite[list_projet[i]]
    df_res.loc[i,"Reception DAACT"] = dic_KeyNom_ValueReceptionDAACT[list_projet[i]]


for key,value in dic_nomTacheV_cleTacheV.items(): # parcourir
    df_res["NB_"+key] = 0 # comiben de attchement dans ce tache
    df_res["conf_"+key] = 0 #
    df_res["perM_"+key] = nan #
    df_res["url_"+key] = nan #
    df_res["Nom_"+key] = nan #
    df_res["createdDate_"+key] = nan #

    # for key,value in dic_nomTacheV_cleTacheV.items():
    #     df_res.loc[i,"NB_"+key] = 0
    #     df_res.loc[i,"conf_"+key] = 0
    #     df_res.loc[i,"perM_"+key] = nan
    #     df_res.loc[i,"url_"+key] = nan
    #     df_res.loc[i,"Nom_"+key] = nan
    #     df_res.loc[i,"createdDate_"+key] = nan
    # ===================================================================
    # observer 
    # ===================================================================

# ===================================================================
# Mis à jour
# ===================================================================
df_res["Mis à jour"]  = df_attachement["Mis à jour"][0]

with open(r"Observation\OB_004_res_vide.csv","w",encoding="utf-8") as f:
    f.write(df_res.to_csv(index=None,line_terminator="\n"))

    # ===================================================================
    # Traverser/parcourir toutes les attachements
    # ===================================================================
for key_attachement,row_attachement in df_attachement.iterrows():
    try:
        # get : nom de attchement 
        nom_attachement = row_attachement['nom_attachement']
        # get : index de Tache ; nom de tache ; lien de tache
        indexTache= df_Tahce[(df_Tahce.id_parent==row_attachement["id_parent"])].index.tolist()[0]
        nom_tache = df_Tahce["nom_parent"][indexTache]
        link_tache = df_Tahce["permalink"][indexTache]

        # get tache_resume
        # 对比任务名称是否含有任务关键字，对应了几个任务，（1）如果只对应一个任务；（2）对应多个任务则对比文件关键字（取最后一个） 
        list_KeyTache_potent = []
        for nomTahce,cleMotTahce in dic_nomTacheV_cleTacheV.items():
            if cleMotTahce.upper() in nom_tache.upper():
                list_KeyTache_potent.append(nomTahce)

        if len(list_KeyTache_potent) == 1:
            tache_resume = list_KeyTache_potent[0]
        elif len(list_KeyTache_potent) >= 2:
            tache_resume = list_KeyTache_potent[0]

            # for nomTahce in list_KeyTache_potent:
            #     for i in dic_nomTacheV_cleAttchementV[nomTahce].split(","):
            #         if i.upper() in nom_attachement.upper():
            #             tache_resume = nomTahce
        else:
            tache_resume = nan

        #si la tache n'est pas vide
        if not pd.isna(tache_resume):
            # get nom de projet；index de projet
            nomProjet_from_tache = re.findall(p1,nom_tache)[0]
            indexProjet = dic_KeyNom_ValueIndex[nomProjet_from_tache]
            # nombre de attachement  +1
            df_res.loc[indexProjet, "NB_"+tache_resume] = 1 + df_res["NB_"+tache_resume][indexProjet]
            df_res.loc[indexProjet, "perM_"+tache_resume] = link_tache
            
            # controle si cette attachement est conforme, 
            # Cela est vrai si les trois conditions suivantes sont remplies
            flag_validation_attechement = True
            # (1) attchement contiens un des cles
            flag_validation_CLE_attechement = False
            for i in dic_nomTacheV_cleAttchementV[tache_resume].split(","):
                if i.upper() in row_attachement['nom_attachement'].upper():
                    flag_validation_CLE_attechement = True
            if not flag_validation_CLE_attechement:
                flag_validation_attechement = False
            # (2) control nom_extension 
            nom_extension = re.sub(u"\\(.*?\\)|\\{.*?}|\\[.*?]", "", row_attachement['nom_attachement']).strip(" ").split(".")[-1]
            if not (nom_extension.lower() in ['pdf',"doc","xls","ods","zip"]):
                flag_validation_attechement = False
            # (3) control nom de url
            if not ("storage.www.wrike.com" in row_attachement['url']):
                flag_validation_attechement = False

            if flag_validation_attechement:
                df_res.loc[indexProjet, "conf_"+tache_resume] = 1 + df_res["conf_"+tache_resume][indexProjet]

                # 2020-11-16T14:55:52Z  createdDate
                if pd.isna(df_res["createdDate_"+tache_resume][indexProjet]):
                    df_res.loc[indexProjet, "url_"+tache_resume] = row_attachement['url']
                    df_res.loc[indexProjet, "createdDate_"+tache_resume] = row_attachement['createdDate']  
                    df_res.loc[indexProjet, "Nom_"+tache_resume] = row_attachement['nom_attachement']
                elif datetime.strptime(df_res["createdDate_"+tache_resume][indexProjet], '%Y-%m-%dT%H:%M:%SZ') < datetime.strptime(row_attachement['createdDate'], '%Y-%m-%dT%H:%M:%SZ'):
                    df_res.loc[indexProjet, "url_"+tache_resume] = row_attachement['url']
                    df_res.loc[indexProjet, "createdDate_"+tache_resume] = row_attachement['createdDate']
                    df_res.loc[indexProjet, "Nom_"+tache_resume] = row_attachement['nom_attachement']

    except:
        # print(row_attachement["nom_attachement"])
        df_exception.loc[index_df_exception,"nom_pdf"] = row_attachement["nom_attachement"]
        index_df_exception +=1

# ===================================================================
# sauvegrader
# ===================================================================
with open(r"Resultat\res_004_Projet_attchement.txt","w",encoding="utf-8") as f:
    f.write(json.dumps( df_res.to_json(),ensure_ascii=False))

with open(r"Resultat\res_004_Exception_attchement.csv","w",encoding="utf-8") as f:
    f.write(df_exception.to_csv(index=None,line_terminator="\n"))






