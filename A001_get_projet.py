import json
import requests
import pandas  as pd
import numpy as np
from pandas.core.frame import DataFrame
from datetime import datetime
import time


def main():
    listChampsCibles = ['📆 GO RACCO', 'Puissance MWc (Prev)', 
    'Date MES', '📆 Constat 3️⃣', 'Lauréat AO', '📆 Date Accord CR(D D)', 
    'Hypothèque', 'CAPEX - V1 (K€)', 'Marge (V1)', '*SPV', 'Date (MES prev)',
    'Type (Tarif)', 'Date paiement acompte ( CR(D) )', 'Marge K€ (V2)', 'Marge (V2)',
    '📅 Date Procuration', '📆 Date (T0)', '📆 GO URBA', '📆 Prev Accord', 
    '📅 Date R1 fixée', '📆 COMPLETUDE OK', '📆 Date (Demande Racco)', 
    'Montant HT CR(D)', 'CAPEX - V2 (K€)', 'Commercial', '📅 Signature PMBAIL',
    'Marge K€ (V1)', 'FIT €/MWh (déf)', 'Marge K€ (V3)', '📅 GO CONSTRUCTION', 
    '📅 GO TARIF', 'Pipeline (HS)', '📆 Constat 1️⃣', '📆 Constat 2️⃣', 'Accord',
        'Responsable Dev', 'Région (Projet)', '📆 Dépôt', 'Estimatif cout Racco (HT)',
        'CAPEX - V3 (K€)', 'Marge (V3)','Date dépôt AO', 'Période (Dépôt CRE)',
        'Type (Tarif)-V2','Date (Limite MES)','Date (Signature Procuration)',
        '📆 Date réception CR(D)','Montant HT CR(D)','Puissance (MWc Déf)',
        'Date (envoi dossier notaire)','Date (Début Chantier)','Date (Fin Chantier)',
        'Date (Debut Structure)','Date (Fin Structure)','Date (Debut Elec)',
        'Date (Fin Elec)','Date (Debut Fondations)','Date (Fin Fondations)',
        'Code Postal (Projet)','Commune (Projet)','Longitude DD (Projet)',
        'Latitude DD (Projet)','Productible kWh/kWc (V1)',
        'Productible kWh/kWc (V2 Solargis)','Productible kWh/kWc (V3 Solargis)',
        'Productible kWh/kWc (V4 Solargis)','Adresse (Projet)','Type (AU)','📆 Signature Bail',
        "Date accord (PCM1)","Date accord (PCM2)","Date accord (PCM3)","Date accord (Transf PC)",
        "📅  Date accord (Transf DP)","Validité","Réception DAACT","Date paiement acompte (PTF)",
        "Date réception (CARDI)","date signature CA","Date (Consuel)"
        ]
    
    '''
    1 tableau
        (1)   'Type (Tarif)-V2' - >filtage (un de AO CRE, AO CRE Innovation),
        (2)     'Date dépôt AO' <-> DOSSIERAO
                    -> tous les date precedentes
        (3) '📆 Date (T0)' <-> LAUREAT
                    - >   'Date dépôt AO'        
                    -> tous les date precedentes
        (4) LAUREAT - > DOSSIERAO
    '''

    '''
    2 tableau pour OA (Type (Tarif)-V2')
        (0) affichage '📆 Date (Demande Racco)', sans controle
        (1)   '📆 Date (T0)' <->"DDROA"
                                        -> tous les date precedentes

    atache 4e tableau
    '''

    '''
    3 tableau pour CRE (Type (Tarif)-V2')
        (0) affichage '📆 Date (T0)', sans controle
        (1)   '📆 Date (Demande Racco)' <->"DDRCRE"
                                        -> tous les date precedentes
    
    attcher 4e tableau
    '''

    '''
    4 page
        (0) sans affichage. sans filtrage
        (1) Date Réception PTF  <-> PTF
            -> tous les date precedentes
        (2)'📆 Date réception CR(D)' < - > "CRD"
            -> tous les date precedentes
        (3) il y  pas de control entre Date Réception PTF, '📆 Date réception CR(D)'
        (4) "Date paiement acompte (PTF)" <-> ACOMPTEPTF
            -> tous les date precedentes
        (5)'Date paiement acompte ( CR(D) )' <-> ACOMPTECRD
            -> tous les date precedentes
        (6) il y  pas de control entre (4) (5)
    '''

    '''
    5 page
       (1) Date réception (CARDI) <-> "CARDI"
                -> tous les date precedentes
        (2) 'Date MES'  <-> "MES"
                ->"CONSUELV1" ou "CONSUELV2","DOEV1"ou "DOEV2","PVRV1" ou "PVRV2"
                -> tous les date precedentes
        (3) "date signature CA" <-> "CONTRATHA"
                -> tous les date precedentes
    
    '''

    '''
    6 page 
        (0) si ,"DOEV2" ou"DOEV1" -> '📅 GO CONSTRUCTION'
        () "DOEV1" -> not "DOEV2" ,"DOEV2" -> not "DOEV1"
        () PVRV1 -> not PVRV2, PVRV2 -> not PVRV1
        (1) Date (Consuel) < - > existance un entre "CONSUELV1" et "CONSUELV2" 
                                -> tous les date precedentes (sauf MES et )
        (2) "CONSUELV1" - > not "CONSUELV2" 
        (3) "CONSUELV2" -> not "CONSUELV1"
        (4) affichage 'Date (Fin Chantier)'

    '''

    '''
    7 page tout champs
    les segement a cote 
    '''

    # ==========================================
    # preparation des parametres
    # ==========================================
    myToken = "eyJ0dCI6InAiLCJhbGciOiJIUzI1NiIsInR2IjoiMSJ9.eyJkIjoie1wiYVwiOjI3MjIxMjUsXCJpXCI6ODM5MzMyMixcImNcIjo0NjM1NDQxLFwidVwiOjE0ODk5ODQzLFwiclwiOlwiVVNcIixcInNcIjpbXCJXXCIsXCJGXCIsXCJJXCIsXCJVXCIsXCJLXCIsXCJDXCIsXCJEXCIsXCJNXCIsXCJBXCIsXCJMXCIsXCJQXCJdLFwielwiOltdLFwidFwiOjB9IiwiaWF0IjoxNjY0NzgzNzg3fQ.bJxVIxGToNC5RLHamvi8Ckj--Ug2-LSp2Kf0d9DS8Nc"
    url = 'https://www.wrike.com/api/v4'
    # ==========================================
    # get id des champs par API, en verifiant les champs cible
    # get log_champs, qui present les champs non trouves 
    # ==========================================
    reponse = requests.get(url + "/customfields",   
                    headers = {'Authorization':  'bearer ' + myToken },)
        # ==========================================
        # get id des champs, en verifiant les champs cible 
        # ==========================================
    reponse_id_nom_champs =reponse.json()['data']
    dic_id_nom_champs ={i["id"]:i["title"] for i in  reponse_id_nom_champs  if i["title"] in listChampsCibles}
        # ==========================================
        # get log_champs, qui present les champs non trouves 
        # ==========================================
    list_champ_trouve = list(dic_id_nom_champs.values())
    list_champ_non_trouve = [i for i in listChampsCibles if i not in list_champ_trouve]
    log_champs_non_trouve = ",".join(list_champ_non_trouve)
#     print(log_champs_non_trouve)

    # ==========================================
    # get les projet de 0032 par API,
    # ==========================================
    id_dossier_0032 = "IEACTCKNI4LT5UCB"
    reponse = requests.get(url + '/folders/'   + id_dossier_0032 +"/folders" + "?project=true&fields=[customFields]", 
                headers = {'Authorization':  'bearer ' + myToken })

    with open("OB_000_reponse_les_projet"+".txt","w",encoding="utf-8") as f:
        f.write(reponse.text)

    df_table = DataFrame()
    index = 0
    log_get_valeur_champs = " "
    for i in reponse.json()['data']:
        try:        
            df_table.loc[index,"work_Id"] = i["id"]
            df_table.loc[index,"nom projet"] = i["title"]
            df_table.loc[index,"permalink"] = i["permalink"]
            df_table.loc[index,"Date de création"] = i['project']['createdDate']

            IdetValue_curentProjet = i['customFields']
            dic_IdetValue_curentProjet = {u['id']:u['value'] for u in  IdetValue_curentProjet}

            for id_champs,value_champs in dic_IdetValue_curentProjet.items():
                if id_champs in list(dic_id_nom_champs.keys()):
                    df_table.loc[index,dic_id_nom_champs[id_champs]] = dic_IdetValue_curentProjet[id_champs].replace('\n','')
            index +=1
        except:
            print(i["title"]) 

    # ==========================================
    # get nom de dev 
    # ==========================================
    df_table['Responsable Dev ID'] = df_table['Responsable Dev'][:]
    df_table['Responsable Dev'] = ''

    id_ResP_dist = df_table['Responsable Dev ID'].drop_duplicates()
    print(id_ResP_dist)
    dic_Resp ={}
    for i in id_ResP_dist:
        try: 
            curentIdUser = i.split(',')[0]
            x = requests.get(url + "/users/" + curentIdUser,    # recuperer les valeur et les IDs des champs personnalises dans ce projet
                headers = {'Authorization':  'bearer ' + myToken })    
            dic_Resp[curentIdUser] =  x.json()['data'][0]['lastName'] +' ' + x.json()['data'][0]['firstName']
        except:
            dic_Resp[curentIdUser] = 'Anonyme'

    for key, rowProjet in df_table.iterrows():  # parcourir tous les projet dans le tableaux/dataframe
        try:
            curentIdUser = str(rowProjet['Responsable Dev ID']).split(',')[0]
            df_table.loc[key,'Responsable Dev'] = dic_Resp[curentIdUser]
        except:
            testErr = ' '

    with open(r"Resultat\res_001_Projet0032.txt","w",encoding="utf-8") as f:
        f.write(json.dumps( df_table.to_json(),ensure_ascii=False))

    with open(r"Resultat\res_001_Projet0032.csv","w",encoding="utf-8") as f:
        f.write(df_table.to_csv(index=None,line_terminator="\n"))
main()