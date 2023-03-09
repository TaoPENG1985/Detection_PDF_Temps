import os
import time
"""
Lancer tous les programmes et envoye un email au responsable
"""
# ===================================================================
# Liste des fichiers à executer 
# ===================================================================
list_python = [ 
    "A001_get_projet.py",
    "A002_get_attachement.py",
    "A003_get_tache0032.py",
    "A004_combine_1_2_3.py",
    "A005_Contre_date_pdf.py",
    "A006_sharepoint.py"

                ]
# ===================================================================
# Préparer les logs
# ===================================================================
log = "lanceer : " + time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())) + "\n"

# ===================================================================
# Lancer un par un les programmes
# ===================================================================
for nomPython in list_python:
    commend_python = ('python ' + nomPython)
    p=os.system(commend_python)
    # ===================================================================
    # Si pas de problème alors "marche ok vvvvvvvvvvvvvvvvvvvvvvvv" sinon "marche pas !!!!!!!!!!!!!!!!!!!!!!!"
    # ===================================================================
    if p == 0 :
        log += nomPython + " marche ok vvvvvvvvvvvvvvvvvvvvvvvv " + time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())) + "\n"
    else:
        log += nomPython + " marche pas !!!!!!!!!!!!!!!!!!!!!!! " + "\n"


sharepointUsername = "tao.peng@enoe-energie.fr"
sharepointPassword = "Enoe2022++"

print(log)

with open("log.txt","w",encoding="utf-8") as f:
    f.write(log)