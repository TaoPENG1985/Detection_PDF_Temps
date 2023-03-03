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
# recuperer projet dans 0032, attachements des Wrike,  Tahce de 0032
# ===================================================================
df_projet = get_text_to_df(r'Resultat\res_001_Projet0032.txt')
df_attachement =  get_text_to_df( r'Resultat\res_002_attachement.txt')
df_Tahce = get_text_to_df( r'Resultat\res_003_id_nom_Tahce.txt')

# ===================================================================
# pre-Filtrez les tache: si une tache contient des mot cle, elle est valide
# ===================================================================
# for key_tache,row_tache in df_Tahce.iterrows():
#     print(key_tache)
#     for key_tacheV,cleTacheV in dic_nomTacheV_cleTacheV.items():
#         if cleTacheV.upper() in row_tache["nom_parent"].upper():
#             df_Tahce.loc[key_tache,"valide"] = 1
#             break
# df_Tahce = df_Tahce.drop(df_Tahce[(df_Tahce['valide'] != 1)].index) 
# # observer 
# with open("OB_003_attachement_1.csv","w",encoding="utf-8") as f:
#     f.write(df_attachement.to_csv(index=None,line_terminator="\n"))

# ===================================================================
# pre-Filtrez les attachement: Si l'identifiant de leur père (tâche) est en 0032, 
# si oui , gardez-le; si non, le supprimer. 
# ===================================================================
# for key_attachement,row_attachement in df_attachement.iterrows():
#     index_parentID = df_Tahce[(df_Tahce.id_parent==row_attachement["id_parent"])].index.tolist()
#     df_attachement.loc[key_attachement,"valide"] = len(index_parentID)
# df_attachement = df_attachement.drop(df_attachement[(df_attachement['valide'] != 1)].index) 
# df_attachement.reset_index(drop=True, inplace=True)
# # observer 
# with open("OB_003_attachement_1.csv","w",encoding="utf-8") as f:
#     f.write(df_attachement.to_csv(index=None,line_terminator="\n"))

# ===================================================================
# les preparation avant la combination
# ===================================================================
p1 = re.compile(r'[[](.*?)[]]', re.S) # preparer un instance de 《re》 pour recuprer les text dans [] 

dic_champs_cible_newNom = {     "nom projet":"nom projet",
                                'work_Id':"work_Id",
                                '*SPV':"SPV",
                                'permalink':"permalink",
                                'Type (AU)':"Type (AU)",
                                "Responsable Dev":"Responsable",

                                '📅 Signature PMBAIL':"date_PMBAIL",
                                '📆 Signature Bail':"date_BAIL",
                                'Accord':"date_Accord",
                                'Date (Fin Chantier)':"date_fin_Chantier",
                                'Date (Début Chantier)':"date_debut_Chantier",

                                "Date accord (PCM1)":"date accord (PCM1)",
                                "Date accord (PCM2)":"date accord (PCM2)",
                                "Date accord (PCM3)":"date accord (PCM)",

                                "📅  Date accord (Transf DP)": "date accord (Transf DP)",
                                "Date accord (Transf PC)":"date accord (Transf PC)",

                                "Validité":"Validite",
                                "Réception DAACT":"Reception DAACT",

}

# ===================================================================
# Vérifier si les noms des objets sont en double ,Supprimez les doublons s'ils existent
# select des champs et les renomer
# ===================================================================
df_projet = df_projet.drop_duplicates(subset=['nom projet'], keep='first')
df_projet = df_projet.rename(columns=dic_champs_cible_newNom)
df_projet = df_projet[list(dic_champs_cible_newNom.values())]


# ===================================================================
# Initialiser deux table vide
# ===================================================================
df_res =df_projet[:] # 
df_exception = DataFrame() # 
index_df_exception = 0

for key,value in dic_nomTacheV_cleTacheV.items(): # parcourir
    df_res["NB_"+key] = 0 # comiben de attchement dans ce tache
    df_res["conf_"+key] = 0 #
    df_res["perM_"+key] = nan #
    df_res["url_"+key] = nan #
    df_res["Nom_"+key] = nan #
    df_res["createdDate_"+key] = nan #

# ===================================================================
# Traverser/parcourir toutes les attachements, puis remolire <df_res>
# ===================================================================
for key_attachement,row_attachement in df_attachement.iterrows():
    try:
        # ===================================================================
        # Obtenez le nom de la pièce jointe, l'index/le nom/le lien de la tâche attachée à la pièce jointe.
        # ===================================================================
        # get : nom de la pièce jointe
        nom_attachement = row_attachement['nom_attachement']
        # get : l'index/le nom/le lien de la tâche attachée à la pièce jointe.
        index_Tache= df_Tahce[(df_Tahce.id_parent==row_attachement["id_parent"])].index.tolist()[0]
        nom_Tache = df_Tahce["nom_parent"][index_Tache]
        link_Tache = df_Tahce["permalink"][index_Tache]

        # ===================================================================
        # analyse le nom de la tache, est que il contien des mots cle des <tache_resume>
        # selon le dictionnaire <dic_nomTacheV_cleTacheV> 
        # si il y a multipe tache_resume, prendre 1er
        # si rien , tache_resume = nan
        # ===================================================================        
        list_KeyTache_potent = []
        for nomTahce,cleMotTahce in dic_nomTacheV_cleTacheV.items():
            if cleMotTahce.upper() in nom_Tache.upper():
                list_KeyTache_potent.append(nomTahce)
        if len(list_KeyTache_potent) == 1:
            tache_resume = list_KeyTache_potent[0]
        elif len(list_KeyTache_potent) >= 2:
            tache_resume = list_KeyTache_potent[0]
        else:
            tache_resume = nan
        # ===================================================================
        #   si la tache n'est pas vide,
        # ===================================================================  
        if not pd.isna(tache_resume):
            # ===================================================================
            # get (1) nom (2) index de projet, (3) Augmentez 1 de le nombre de pièces jointes (4)get lien de tache
            # ===================================================================  
            nomProjet_from_tache = re.findall(p1,nom_Tache)[0]  # get nom de projet (le nom de tache contient son son de projet dans [])
            indexProjet = df_projet[df_projet['nom projet']== nomProjet_from_tache].index.tolist()             # get index de projet
            df_res.loc[indexProjet, "NB_"+tache_resume] = 1 + df_res["NB_"+tache_resume][indexProjet] #(3) Augmentez 1 de le nombre de pièces jointes 
            df_res.loc[indexProjet, "perM_"+tache_resume] = link_Tache #(4)get lien de tache

            # ===================================================================
            # controle si cette attachement est conforme, 
            # Cela est vrai si les trois conditions suivantes sont remplies
            # ===================================================================             
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

            # ===================================================================
            # si cette attachement est conforme, 
            # (1) Augmentez le nombre de pièces jointes conformes 1
            # (2) mis a jour la <createdDate> de cette tache_resume，le nom de cette attachement,
            #    la lien de telechargement de  cette attachement
            # ===================================================================   
            if flag_validation_attechement:
                # (1) Augmentez le nombre de pièces jointes conformes 1
                df_res.loc[indexProjet, "conf_"+tache_resume] = 1 + df_res["conf_"+tache_resume][indexProjet]
                # (2) mis a jour la <createdDate> de cette tache_resume，le nom de cette attachement,
                #       la lien de telechargement de  cette attachement
                if pd.isna(df_res["createdDate_"+tache_resume][indexProjet]):
                    df_res.loc[indexProjet, "url_"+tache_resume] = row_attachement['url']
                    df_res.loc[indexProjet, "createdDate_"+tache_resume] = row_attachement['createdDate']  
                    df_res.loc[indexProjet, "Nom_"+tache_resume] = row_attachement['nom_attachement']
                elif datetime.strptime(df_res["createdDate_"+tache_resume][indexProjet], '%Y-%m-%dT%H:%M:%SZ') < datetime.strptime(row_attachement['createdDate'], '%Y-%m-%dT%H:%M:%SZ'):
                    df_res.loc[indexProjet, "url_"+tache_resume] = row_attachement['url']
                    df_res.loc[indexProjet, "createdDate_"+tache_resume] = row_attachement['createdDate']
                    df_res.loc[indexProjet, "Nom_"+tache_resume] = row_attachement['nom_attachement']
        # ===================================================================
        # Collecte des pièces jointes qui ne peuvent pas être traitées
        # ===================================================================    
    except:
        df_exception.loc[index_df_exception,"nom_pdf"] = row_attachement["nom_attachement"]
        index_df_exception +=1

# ===================================================================
# Mis à jour
# ===================================================================
df_res["Mis à jour"]  = df_attachement["Mis à jour"][0]
# ===================================================================
# sauvegrader
# ===================================================================
with open(r"Resultat\res_004_Projet_attchement.txt","w",encoding="utf-8") as f:
    f.write(json.dumps( df_res.to_json(),ensure_ascii=False))

with open(r"Resultat\res_004_Exception_attchement.csv","w",encoding="utf-8") as f:
    f.write(df_exception.to_csv(index=None,line_terminator="\n"))






