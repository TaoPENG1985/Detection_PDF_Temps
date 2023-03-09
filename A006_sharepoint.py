
from shareplum import Site
from shareplum import Office365
from shareplum.site import Version
"""
Récuperer les resultat au format csv et les envoyer à SharePoint
"""
# ===================================================================
# preparation des Chemin , Authentication
# ===================================================================
sharepointUsername = "tao.peng@enoe-energie.fr"
sharepointPassword = "Enoe2022++"
sharepointSite = "https://enoeenergie.sharepoint.com/sites/Service.SI"
website = "https://enoeenergie.sharepoint.com"
authcookie = Office365( website,
                        username=sharepointUsername,
                        password=sharepointPassword).GetCookies()
site = Site(sharepointSite,version=Version.v365, authcookie=authcookie)

# ===================================================================
# Indiquer le lieu de stockage
# ===================================================================
# folder = site.Folder("Documents%20partages/Donnes_Rapport_Direction")
folder = site.Folder("Documents%20partages/08-Power BI/Jeu_de_donnees_Rapport_DataRoom")

# ===================================================================
# Appuyez sur le fichier dans la liste pour le stocker
# ===================================================================
list_file = [
            "Resultat/res_004_Exception_attchement.csv",
            "Resultat/res_005_controle_date_pdf.csv"
]

# ===================================================================
# Envoyer les fichiers dans SharePoint
# ===================================================================
for file_name in list_file:
    with open(file_name, "rb") as f:
        file_content = f.read() 
        folder.upload_file(file_content, file_name.split("/")[-1])
