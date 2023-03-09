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
# Si le parent est de type string et contient le fils qui est de type string alors on met flage_validation à true
# sinon on met flage_validation à false
# ===================================================================
def try_contien_boolen(parent,fils):
    flage_validation = False
    try:
        if fils in parent:
            flage_validation = True
    except:
        flage_validation =False
    return  flage_validation

# ==================================================================
# Vérifier que pour chaque date il y a des pdfs associés
# Vérifier que pour un ensemble de pdf il y a une date associée
# Vérifier que pour chaque date il y a une date précédantes
# ===================================================================
def control_date_pdf_datePrecedent(date_control,
                                   list_pdf,
                                   list_date_precedent,
                                   condition_Champ = "",
                                   condition_operation = "=",
                                   conditon_valeur = ""):
    for key_projet,row_projet in df_projet_lien.iterrows(): # parcourir les projet
        # ===================================================================
        #  Vérifier si condition_operation est égale à "==" ou à "contient"
        #  (1) Si "==" alors si la valeur de condition_Champ n'est pas egal à conditon_valeur
        #    alors on met condition_Flag à False
        #  (2) Si "Contient" alors si la valeur de condition_Champ ne contient pas le conditon_valeur
        #    alors on met condition_Flag à False 
        # ===================================================================
        condition_Flag = True
        if condition_Champ !="":
            if condition_operation == "==":
                if row_projet[condition_Champ] != conditon_valeur:
                    condition_Flag = False
            if condition_operation == "Contient":
                if not try_contien_boolen(row_projet[condition_Champ],conditon_valeur):
                    condition_Flag = False

        # ==================================================================
        # date - > pdf :  
        # Si une lignes répond à une certaine condition
        # et si date_control n'est pas vide
        # alors nous vérifierons si tous les pdf sont présents.
        # ===================================================================
        if condition_Flag and (not pd.isna(row_projet[date_control])):
            for i in list_pdf:
                
                flag_est_list = False
                if type(i) == type([]):
                    flag_est_list = True
                elif type(i) == type(""):
                    flag_est_list = False

                if flag_est_list:
                    if row_projet["NB_"+i[0]] == 0 and row_projet["NB_"+i[1]] == 0:
                        df_projet_lien.loc[key_projet,"tache_" +i[0]] = "0 ❌"
                        df_projet_lien.loc[key_projet,"tache_" +i[1]] = "0 ❌"
                    if row_projet["conf_"+i[0]] == 0 and row_projet["conf_"+i[1]] == 0:
                        df_projet_lien.loc[key_projet,"fichier_" +i[0]] = "0 ❌"
                        df_projet_lien.loc[key_projet,"fichier_" +i[1]] = "0 ❌"      
                else:
                    if row_projet["NB_"+i] == 0:
                        df_projet_lien.loc[key_projet,"tache_" +i] = "0 ❌"
                    if row_projet["conf_"+i] == 0:
                        df_projet_lien.loc[key_projet,"fichier_" +i] = "0 ❌"
                   
        # ==================================================================
        # pdf - > date : 
        # Si une lignes répond à une certaine condition
        # si tous les pdf sont presents, 
        # alors nous vérifierons si la date_control esy présent.
        # ===================================================================
        if condition_Flag:
            # Verifer si tous les pdfs sont presents, avec flag_tous_pdf_prensent
            flag_tous_pdf_prensent = True # 
            for i in list_pdf:

                flag_est_list = False
                if type(i) == type([]):
                    flag_est_list = True

                if flag_est_list:
                    if row_projet["NB_"+i[0]] == 0 and row_projet["NB_"+i[1]] == 0:
                        flag_tous_pdf_prensent = False
                else:
                    if row_projet["NB_"+i] == 0:
                        flag_tous_pdf_prensent = False
            # si tous les pdfs sont presents, et date_control est abesnt, on le marque "❌ fichiers"
            if flag_tous_pdf_prensent:
                if pd.isna(df_projet_lien.loc[key_projet,date_control]):
                    df_projet_lien.loc[key_projet,date_control] = "❌ fichiers"

        # ==================================================================
        # date - > date precedent: 
        # Si la date_control est présent.
        # Nous vérifierons si les date precedents sont présentes.
        # ===================================================================
        if condition_Flag:
            if not pd.isna(df_projet_lien.loc[key_projet,date_control]):
                for j in list_date_precedent:
                    if pd.isna(df_projet_lien.loc[key_projet,j]):
                        df_projet_lien.loc[key_projet,j] = "❌ " + date_control

# ===================================================================
# control_unCouple_conflit_Pdf vérifie la logique : 
# dans une paire de fichiers, les occurrences sont mutuellement exclusives. 
# Si deux fichiers apparaissent en même temps, on dit qu'une erreur s'est produite.
# ===================================================================
def control_unCouple_conflit_Pdf(list_couple_pdf):
    for key_projet,row_projet in df_projet_lien.iterrows(): # parcourir les projet
        for couple_confit in list_couple_pdf:
            if row_projet["NB_"+couple_confit[0]] > 0:
                if row_projet["NB_"+couple_confit[1]] > 0:
                    df_projet_lien.loc[key_projet,"tache_" +couple_confit[0]] = str(row_projet["NB_"+couple_confit[0]]) + " ❌ conflit"
                    df_projet_lien.loc[key_projet,"tache_" +couple_confit[1]] = str(row_projet["NB_"+couple_confit[1]]) + " ❌ conflit"
            if row_projet["conf_"+couple_confit[0]] > 0:
                if row_projet["conf_"+couple_confit[1]] > 0:
                    df_projet_lien.loc[key_projet,"fichier_" +couple_confit[0]] = str(row_projet["conf_"+couple_confit[0]]) + " ❌ conflit"
                    df_projet_lien.loc[key_projet,"fichier_" +couple_confit[1]] = str(row_projet["conf_"+couple_confit[1]]) + " ❌ conflit"


# ===================================================================
# les "tache_", "fichier_" pour afficher dans power BI
# ===================================================================
for tache_resume in dic_nomTacheV_cleAttchementV.keys():
    for i in ["tache_", "fichier_"]:
        df_projet_lien[i+tache_resume] = nan


# ==================================================================
# CONSUELV1 -> not CONSUELV2,  CONSUELV2 -> not CONSUELV1
# "DOEV1" -> not "DOEV2" ,"DOEV2" -> not "DOEV1"
# "PVRV1 -> not PVRV2, PVRV2 -> not PVRV1
# ===================================================================
list_couple_pdf =[  ["CONSUELV1" ,"CONSUELV2"],
                    ["DOEV1","DOEV2"],
                    ["PVRV1","PVRV2"]]
control_unCouple_conflit_Pdf(list_couple_pdf)


# 15 ===================================================================
# Date signature CA <-> pdf:["CONTRATHA"]  
# -> date précédent: ["Date MES","Date Consuel", "date_fin_Chantier","date_debut_Chantier", "Date GO CONSTRUCTION",
#  "date_BAIL","Date T0","Date Demande Racco","date_Accord" ,"Date depot Accord","Date GO URBA","date_PMBAIL"]
# =================================================================== 
date_control = "Date signature CA"
list_pdf_present = ["CONTRATHA"]
list_date_prededente = ["Date MES","Date Consuel", "date_fin_Chantier","date_debut_Chantier", "Date GO CONSTRUCTION",
     "date_BAIL","Date T0","Date Demande Racco","date_Accord" ,"Date depot Accord","Date GO URBA","date_PMBAIL"]
control_date_pdf_datePrecedent(date_control,list_pdf_present,list_date_prededente)

# 14 ==================================================================
# 'Date MES' <-> pdf: ["MES", 
#                              un de ["CONSUELV1","CONSUELV2"],
#                              un de ["DOEV1","DOEV2"],
#                               un de ["PVRV1","PVRV2"]]
# -> date précédent: ["Date Consuel",  "date_fin_Chantier","date_debut_Chantier", "Date GO CONSTRUCTION",
#     "date_BAIL","Date T0","Date Demande Racco","date_Accord"  ,"Date depot Accord" ,"Date GO URBA","date_PMBAIL"]
# =================================================================== 
date_control = "Date MES"
list_pdf_present = ["MES",
                          ["CONSUELV1","CONSUELV2"],
                          ["DOEV1","DOEV2"],
                          ["PVRV1","PVRV2"]]
list_date_prededente = ["Date Consuel",  "date_fin_Chantier","date_debut_Chantier", "Date GO CONSTRUCTION",
     "date_BAIL","Date T0","Date Demande Racco","date_Accord"  ,"Date depot Accord" ,"Date GO URBA","date_PMBAIL"]
control_date_pdf_datePrecedent(date_control,list_pdf_present,list_date_prededente)

# 13 ===================================================================
# 'Date Consuel' <-> pdf: un de ["CONSUELV1","CONSUELV2"]  
# -> date précédent: [  "date_fin_Chantier","date_debut_Chantier", "Date GO CONSTRUCTION",
#  "date_BAIL","Date T0","Date Demande Racco","date_Accord" ,"Date depot Accord" ,"Date GO URBA","date_PMBAIL"]
# =================================================================== 
date_control = "Date Consuel"
list_pdf_present = [["CONSUELV1","CONSUELV2"]]
list_date_prededente = [  "date_fin_Chantier","date_debut_Chantier", "Date GO CONSTRUCTION",
     "date_BAIL","Date T0","Date Demande Racco","date_Accord" ,"Date depot Accord" ,"Date GO URBA","date_PMBAIL"]
control_date_pdf_datePrecedent(date_control,list_pdf_present,list_date_prededente)

# 12 ===================================================================
# date de fin chantier <-> pdf (Pour PC : ["DOC","DAACT","CONSTATPC","CNRPC","PRODPC","ARRETEPC"]
#                               Pour DP : ["DAACT","CONSTATDP","CNRDP","PRODDP","ARRETEDP"]
#                               Pour PD+DP: ["DOC","DAACT","CONSTATPC","CNRPC","CONSTATDP","CNRDP",
#                                                  "PRODPC","ARRETEPC","PRODDP","ARRETEDP"] )
# - > date précédent: ["date_debut_Chantier", "Date GO CONSTRUCTION",
#  "date_BAIL","Date T0","Date Demande Racco","date_Accord" ,"Date depot Accord" ,"Date GO URBA","date_PMBAIL"]
# ===================================================================
date_control = "date_fin_Chantier"
list_date_prededente = ["date_debut_Chantier", "Date GO CONSTRUCTION", "date_BAIL",
                         "Date T0","Date Demande Racco","date_Accord" ,"Date depot Accord" ,"Date GO URBA","date_PMBAIL"]

# Pour PC
champ = "Type (AU)"
oper = "="
valeur = "PC"
list_pdf_present = ["DOC","DAACT","CONSTATPC","CNRPC","PRODPC","ARRETEPC"]
control_date_pdf_datePrecedent(date_control,list_pdf_present,list_date_prededente,
                               condition_Champ=champ,condition_operation=oper,conditon_valeur=valeur)


# Pour DP
champ = "Type (AU)"
oper = "="
valeur = "DP"
list_pdf_present = ["DAACT","CONSTATDP","CNRDP","PRODDP","ARRETEDP"]
control_date_pdf_datePrecedent(date_control,list_pdf_present,list_date_prededente,
                               condition_Champ=champ,condition_operation=oper,conditon_valeur=valeur)

# Pour PC+ DP
champ = "Type (AU)"
oper = "="
valeur = "PC+DP"
list_pdf_present = ["DOC","DAACT",
                          "CONSTATPC","CNRPC","CONSTATDP","CNRDP",
                            "PRODPC","ARRETEPC","PRODDP","ARRETEDP"]
control_date_pdf_datePrecedent(date_control,list_pdf_present,list_date_prededente,
                               condition_Champ=champ,condition_operation=oper,conditon_valeur=valeur)


# 11 ===================================================================
# date debut chantier <-> pdf: ["DOC"]
# ->date precedents [ "Date GO CONSTRUCTION",
#     "date_BAIL","Date T0","Date Demande Racco","date_Accord" ,"Date depot Accord" ,"Date GO URBA","date_PMBAIL"]
# ===================================================================
date_control = "date_debut_Chantier"
list_pdf_present = ["DOC"]
list_date_prededente = [ "Date GO CONSTRUCTION","date_BAIL","Date T0",
                         "Date Demande Racco","date_Accord" ,"Date depot Accord" ,
                         "Date GO URBA","date_PMBAIL"]
control_date_pdf_datePrecedent(date_control,list_pdf_present,list_date_prededente)

# 10 ===================================================================
# 'Date GO CONSTRUCTION' <-> pdf: []
# -> date précédent: [ "date_BAIL","Date T0",
#                       "Date Demande Racco","date_Accord" ,"Date depot Accord" ,"Date GO URBA","date_PMBAIL"]
# =================================================================== 
date_control = "Date GO CONSTRUCTION"
list_pdf_present = []
list_date_prededente =[ "date_BAIL","Date T0", "Date Demande Racco","date_Accord" ,
                        "Date depot Accord" ,"Date GO URBA","date_PMBAIL"]
control_date_pdf_datePrecedent(date_control,list_pdf_present,list_date_prededente)


# 9 ===================================================================
# "date Bail" <-> pdf (["SBN"]) 
# -> date précédent: ["Date T0","Date Demande Racco","date_Accord" ,"Date depot Accord" ,
#                       "Date GO URBA","date_PMBAIL"]
# ===================================================================
date_control = "date_BAIL"
list_pdf_present = ["SBN"]
list_date_prededente = ["Date T0","Date Demande Racco","date_Accord" ,"Date depot Accord" ,
                         "Date GO URBA","date_PMBAIL"]
control_date_pdf_datePrecedent(date_control,list_pdf_present,list_date_prededente)

# 8.2 PTF ===================================================================
# 'Date paiement acompte PTF' <-> pdf:"ACOMPTEPTF"  
# -> date précédent: ["Date Reception PTF","Date Demande Racco","Date T0","date_Accord",
#                        "Date depot Accord" ,"Date GO URBA""date_PMBAIL"]
# ===================================================================
date_control = "Date paiement acompte PTF"
list_pdf_present = ["ACOMPTEPTF"]
list_date_prededente =["Date Reception PTF","Date Demande Racco","Date T0","date_Accord",
                        "Date depot Accord" ,"Date GO URBA","date_PMBAIL"]
control_date_pdf_datePrecedent(date_control,list_pdf_present,list_date_prededente)

# 7.2 PTF ===================================================================
# 'Date Reception PTF' <-> pdf:"PTF"  
# -> date précédent: ["Date Demande Racco","Date T0","date_Accord","Date depot Accord" ,
#                        "Date GO URBA","date_PMBAIL"]
# ===================================================================
date_control = 'Date Reception PTF'
list_pdf_present = ["PTF"]
list_date_prededente =["Date Demande Racco","Date T0","date_Accord","Date depot Accord" ,
                        "Date GO URBA","date_PMBAIL"]
control_date_pdf_datePrecedent(date_control,list_pdf_present,list_date_prededente)

# 8.1 -(CRD)===================================================================
# 'Date paiement acompte CR(D)' <-> pdf:"ACOMPTECRD"  
# -> date précédent: ["Date reception CR(D)","Date Demande Racco","Date T0","date_Accord",
#                        "Date depot Accord" ,"Date GO URBA","date_PMBAIL"]
# ===================================================================
date_control = 'Date paiement acompte CR(D)'
list_pdf_present = ["ACOMPTECRD"]
list_date_prededente = ["Date reception CR(D)","Date Demande Racco","Date T0","date_Accord",
                        "Date depot Accord" ,"Date GO URBA","date_PMBAIL"]
control_date_pdf_datePrecedent(date_control,list_pdf_present,list_date_prededente)

# 7.1 -CR(D) ===================================================================
# 'Date reception CR(D)' <-> pdf:"CRD"  -> date précédent: ",
# -> date précédent: ["Date Demande Racco","Date T0","date_Accord","Date depot Accord" ,
#                        "Date GO URBA","date_PMBAIL"]
# ===================================================================
date_control = 'Date reception CR(D)'
list_pdf_present = ["CRD"]
list_date_prededente =["Date Demande Racco","Date T0","date_Accord","Date depot Accord" ,
                        "Date GO URBA","date_PMBAIL"]
control_date_pdf_datePrecedent(date_control,list_pdf_present,list_date_prededente)


# 6.2， ===========================================================
# En cas de "Type Tarif" contient "CRE"
# 'Date T0' <-> pdf:LAUREAT  
# -> date précédent: ["date_Accord","Date depot Accord" , "Date GO URBA","date_PMBAIL"]
# ===================================================================
# 'Type (Tarif)-V2' => {"Obligation d'achat S17", nan, 'Autoconsommation', 'AO CRE', "Obligation d'achat S21", None}
champ= "Type Tarif"
oper = "Contient"
valeur = "CRE" 

date_control = "Date T0"
list_pdf_present = ["LAUREAT"]
list_date_prededente = ["date_Accord","Date depot Accord" , "Date GO URBA","date_PMBAIL"]
control_date_pdf_datePrecedent(date_control,list_pdf_present,list_date_prededente,
                               condition_Champ=champ,condition_operation=oper,conditon_valeur=valeur)


# 6.1 =================================================================
#  En cas de "Type Tarif" contient "Obligation d'achat"
# 'Date T0' <-> pdf:["DDROA"]  
# -> date précédent: ["date_Accord","Date depot Accord" , "Date GO URBA","date_PMBAIL"]
# ===================================================================
# 'Type (Tarif)-V2' => {"Obligation d'achat S17", nan, 'Autoconsommation', 'AO CRE', "Obligation d'achat S21", None}
champ= "Type Tarif"
oper = "Contient"
valeur = "Obligation d'achat" 

date_control = "Date T0"
list_pdf_present = ["DDROA"]
list_date_prededente = ["date_Accord","Date depot Accord" , "Date GO URBA","date_PMBAIL"]

control_date_pdf_datePrecedent(date_control,list_pdf_present,list_date_prededente,
                               condition_Champ=champ,condition_operation=oper,conditon_valeur=valeur)

# 5.2 ===================================================================
# En cas de "Type Tarif" contient "CRE"
# 'Date Demande Racco' <-> pdf:"DDRCRE" 
#  -> date précédent: ["date_Accord","Date depot Accord" , "Date GO URBA","date_PMBAIL"]
# ===================================================================
# 'Type (Tarif)-V2' => {"Obligation d'achat S17", nan, 'Autoconsommation', 'AO CRE', "Obligation d'achat S21", None}
champ= "Type Tarif"
oper = "Contient"
valeur = "CRE" 

date_control = "Date Demande Racco"
list_pdf_present = ["DDRCRE"]
list_date_prededente =["date_Accord","Date depot Accord" , "Date GO URBA","date_PMBAIL"]
control_date_pdf_datePrecedent(date_control,list_pdf_present,list_date_prededente,
                               condition_Champ=champ,condition_operation=oper,conditon_valeur=valeur)


# 5.1 ===============================================================
# En cas de "Type Tarif" contient "Obligation d'achat"
# 'Date Demande Racco' <-> pdf:[]
#  -> date précédent :  ["date_Accord","Date depot Accord" , "Date GO URBA","date_PMBAIL"]
# ===================================================================
# 'Type (Tarif)-V2' => {"Obligation d'achat S17", nan, 'Autoconsommation', 'AO CRE', "Obligation d'achat S21", None}
champ= "Type Tarif"
oper = "Contient"
valeur = "Obligation d'achat" 

date_control = "Date Demande Racco"
list_pdf_present = []
list_date_prededente = ["date_Accord","Date depot Accord" , "Date GO URBA","date_PMBAIL"]
control_date_pdf_datePrecedent(date_control,list_pdf_present,list_date_prededente,
                               condition_Champ=champ,condition_operation=oper,conditon_valeur=valeur)

# 4 ===================================================================
#  6 mois + date Accord  -> pdf (si PC :["CONSTATPC","CNRPC"]
#                                  si DP : ["CONSTATDP","CNRDP"]
#                                   si PC + DP : ["CONSTATPC","CNRPC","CONSTATDP","CNRDP"])
# -> date precedents : []
# ===================================================================
for key_projet,row_projet in df_projet_lien.iterrows(): # parcourir les projet
    list_pdf_present = []
    # de date - > pdf
    if not pd.isna(row_projet["date_Accord"]):
        list_pdf_present = []
        if not("❌" in row_projet["date_Accord"]) and len(row_projet["date_Accord"]) > 1:
            timeStamp = datetime.strptime(row_projet["date_Accord"], "%Y-%m-%d")
            timeStamp_dans6mois = timeStamp + dt.timedelta(days = 180)
            new_Today = datetime.now()
            if timeStamp_dans6mois < new_Today:
                df_projet_lien.loc[key_projet,"+6mois"] = timeStamp_dans6mois.strftime('%Y-%m-%d')
                if "PC" == row_projet["Type (AU)"]:
                    list_pdf_present += ["CONSTATPC","CNRPC"]
                elif "DP" == row_projet["Type (AU)"] :
                    list_pdf_present += ["CONSTATDP","CNRDP"]
                elif "PC+DP" == row_projet["Type (AU)"]:
                    list_pdf_present += ["CONSTATPC","CNRPC"] +  ["CONSTATDP","CNRDP"]

        for i in list_pdf_present:
            if row_projet["NB_"+i] == 0:
                df_projet_lien.loc[key_projet,"tache_" +i] = "0 ❌"
            if row_projet["conf_"+i] == 0:
                df_projet_lien.loc[key_projet,"fichier_" +i] = "0 ❌"

# 3 =================================================================
# date_Accord <-> pdf (selon si PC : ["PRODPC","ARRETEPC"]
#                           si DP : ["PRODDP","ARRETEDP"]
#                           si PC +DP : ["PRODPC","ARRETEPC","PRODDP","ARRETEDP"])
# - > date precedents : ["Date depot Accord" , "Date GO URBA","date_PMBAIL"]
# ===================================================================
date_control = "date_Accord"
list_date_prededente =["Date depot Accord" , "Date GO URBA","date_PMBAIL"]

# Pour PC
champ = "Type (AU)"
oper = "="
valeur = "PC"
list_pdf_present = ["PRODPC","ARRETEPC"]
control_date_pdf_datePrecedent(date_control,list_pdf_present,list_date_prededente,
                               condition_Champ=champ,condition_operation=oper,conditon_valeur=valeur)

# Pour DP
champ = "Type (AU)"
oper = "="
valeur = "DP"
list_pdf_present =["PRODDP","ARRETEDP"]
control_date_pdf_datePrecedent(date_control,list_pdf_present,list_date_prededente,
                               condition_Champ=champ,condition_operation=oper,conditon_valeur=valeur)

# Pour PC+DP
champ = "Type (AU)"
oper = "="
valeur = "PC+DP"
list_pdf_present = ["PRODPC","ARRETEPC","PRODDP","ARRETEDP"] 
control_date_pdf_datePrecedent(date_control,list_pdf_present,list_date_prededente,
                               condition_Champ=champ,condition_operation=oper,conditon_valeur=valeur)

# 2 =========================================================
# "Date depot Accord <-> pdf : []
# ->  date précédente : ["Date GO URBA","date_PMBAIL"]
# ===================================================================
date_control = "Date depot Accord"
list_pdf_present = []
list_date_prededente = ["Date GO URBA","date_PMBAIL"]
control_date_pdf_datePrecedent(date_control,list_pdf_present,list_date_prededente)
 

# 1 ===================================================================
# date PMbail <-> pdf : ["PMBAIL"] 
# -> date précédente : []
# ===================================================================
date_control = "date_PMBAIL"
list_pdf_present = ["PMBAIL"]
list_date_prededente = []
control_date_pdf_datePrecedent(date_control,list_pdf_present,list_date_prededente)


# ===================================================================
# si Bail n'est pas vide, l'eurrer dans date PMBAIL est supprimée (L'erreur est pardonnée)
# ===================================================================
for key_projet,row_projet in df_projet_lien.iterrows(): # parcourir les projet
    if not pd.isna(row_projet["date_BAIL"]):
        if not "❌" in row_projet["date_BAIL"]:
            if not pd.isna(row_projet["date_PMBAIL"]):      
                if  "❌" in row_projet["date_PMBAIL"]:
                    df_projet_lien.loc[key_projet,"date_PMBAIL"] = "Exempt_Bail"

# ===================================================================
#  2pc -> 1pc, 1pc -> PRODPC,ARRETEPC, flag pour 1pc 2pc
# ===================================================================
list_prorog_1PC = {"PROROG1PC","PROROGCONSTAT1PC","PROROGCNR1PC"}
list_prorog_2PC = {"PROROG2PC","PROROGCONSTAT2PC","PROROGCNR2PC"}

for key_projet,row_projet in df_projet_lien.iterrows(): # parcourir les projet
    # 2pc -> 1pc
    # Si l'une des valeurs du list_prorog_2PC existe, 
    # les valeurs du list_prorog_1PC doivent toutes être remplies.
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
    # 1pc -> PRODPC,ARRETEPC 
    # Si l'une des valeurs du list_prorog_1PC ou list_prorog_2PC existe, 
    # les valeurs du PRODPC,ARRETEPC doivent toutes être remplies.
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
    #  FLAG 1_2_pc, pour filtrage dans PowerBi
    df_projet_lien.loc[key_projet,"flag_1_2_pc"] = str(flag_1PC)

# ===================================================================
# 2dp -> 1dp, 1dp -> PRODP,ARRETEDP,flag pour 1_2_dp
# ===================================================================
list_prorog_1DP = {"PROROG1DP","PROROGCONSTAT1DP","PROROGCNR1DP"}
list_prorog_2DP = {"PROROG2DP","PROROGCONSTAT2DP","PROROGCNR2DP"}

for key_projet,row_projet in df_projet_lien.iterrows(): # parcourir les projet
    #  2DP => 1DP, 
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
    #  1DP => PRODP,ARRETEDP
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
    #  FLAG 1_2_dp
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
#  pour fichier_  (lien ) ： si > 1, num ⚠️ ; = 1, 1 🔗; = 0, nan
#  pour tache_  (lien)： si > 1, num ⚠️ ; = 1, 1 ✅; = 0, nan
# ===================================================================
for key_projet,row_projet in df_projet_lien.iterrows(): # parcourir les projet
    for tache_resume in dic_nomTacheV_cleAttchementV.keys():
        if row_projet["conf_"+tache_resume] == 0 and (not try_contien_boolen(row_projet["fichier_"+ tache_resume],"❌")):
            df_projet_lien.loc[key_projet,"fichier_"+tache_resume] = nan
        elif row_projet["conf_"+tache_resume] == 1:
            df_projet_lien.loc[key_projet,"fichier_"+tache_resume] = "1 🔗"
        elif row_projet["conf_"+tache_resume] > 1:
            df_projet_lien.loc[key_projet,"fichier_"+tache_resume] =  str(row_projet["conf_"+tache_resume]) +" ⚠️"

        if row_projet["NB_"+tache_resume] == 0 and (not try_contien_boolen(row_projet["tache_"+tache_resume],"❌")): 
            df_projet_lien.loc[key_projet,"tache_"+tache_resume] = nan
        elif row_projet["NB_"+tache_resume] == 1 :
            df_projet_lien.loc[key_projet,"tache_"+tache_resume] = "1 ✅"
        elif row_projet["NB_"+tache_resume] > 1:
            df_projet_lien.loc[key_projet,"tache_"+tache_resume] =  str(row_projet["NB_"+tache_resume]) +" ⚠️"


# ===================================================================
# Champs pour les projets qui contiennent CS DP RR,
# Certains pdf dans les champs manquant sont autorisé
# Ajout "projet type" pour sauvegarder dans le dataframe
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
# pour tous les projets, detecte les pdf entré il y a moins de 7 jours
# ===================================================================
for key_projet,row_projet in df_projet_lien.iterrows(): # parcourir les projet
    for tache_resume in dic_nomTacheV_cleAttchementV.keys():
        if not (pd.isna(row_projet["createdDate_"+tache_resume])):
            if datetime.strptime(row_projet["createdDate_"+tache_resume], "%Y-%m-%dT%H:%M:%SZ") > datetime.now() - dt.timedelta(days = 7):
                if df_projet_lien.loc[key_projet,"conf_"+tache_resume] > 0:
                    df_projet_lien.loc[key_projet,"fichier_"+tache_resume] += "📩"



    # df_res["createdDate_"+key] = nan

# ===================================================================
# change format du temps tt mm dd -> dd mm tt (A modifier la methode pour changer de format)+
# objet_date.strftime("%d-%m-%y")
# ===================================================================
#"Date MES","Date Consuel", "date_fin_Chantier","date_debut_Chantier", "Date GO CONSTRUCTION",
   #  "date_BAIL","Date T0","Date Demande Racco","date_Accord" ,"Date depot Accord","Date GO URBA","date_PMBAIL"
list_date_champs =["Date MES","Date Consuel","date_Accord","Date GO CONSTRUCTION","Date T0",\
                   "Date GO URBA","Date depot Accord","Date Demande Racco","+6mois","date_PMBAIL","date_fin_Chantier",\
                     "date_BAIL","date_debut_Chantier","date accord (PCM1)","Date signature CA",\
                     "date accord (PCM2)","date accord (PCM3)","date accord (Transf PC)",\
                        "date accord (Transf DP)","Validite"]
for key_projet,row_projet in df_projet_lien.iterrows(): # parcourir les projet
    for key_date in list_date_champs: # parcourir les combinaison
        if not (pd.isna(row_projet[key_date]) or   "❌" in (row_projet[key_date])): 
            if len(row_projet[key_date]) == 10:
                str_temp = row_projet[key_date]
                df_projet_lien.loc[key_projet,key_date] = str_temp[8:10] + str_temp[4:8] +str_temp[:4]


with open(r"Resultat\res_005_controle_date_pdf.txt","w",encoding="utf-8") as f:
    f.write(json.dumps( df_projet_lien.to_json(),ensure_ascii=False))

with open(r"Resultat\res_005_controle_date_pdf.csv","w",encoding="utf-8") as f:
    f.write(df_projet_lien.to_csv(index=None,line_terminator="\n"))


# df_tete_tableau = DataFrame()
# index = 0

# for i in ["tache_PMBAIL","fichier_PMBAIL","date_PMBAIL"]:
#     df_tete_tableau.loc[index,"nom_tete"] = "PMBAIL"
#     df_tete_tableau.loc[index,"nom_champs"] = i
#     index +=1

# with open(r"C:\Users\tpeng\ENOE ENERGIE\SI - Tao PENG\Projet_detectPDF0032\double_tete_PMBAIL.csv","w",encoding="utf-8") as f:
#     f.write(df_tete_tableau.to_csv(index=None,line_terminator="\n"))
#     f.write



# Verifier le programme car tache_LAUREAT et fichier_LAUREAT vide