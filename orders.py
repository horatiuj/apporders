import os
import fnmatch
import pandas as pd
import numpy as np
import time
import math
from functools import reduce

# import openpyxl


class GetExcelToDepozit(object):

    def __init__(self, path):  # definim atributul path
        self.path = path

    # functia intoarce o lista de fisiere dintr-un anume director

    def find(self, pattern, path):
        result = []
        for root, dirs, files in os.walk(path):
            for name in files:
                if fnmatch.fnmatch(name, pattern):
                    result.append(os.path.join(name))
        return result

    # functie - DEPOZIT - prelucreaza  setul de date tip DEPOZIT

    def set_date_comanda(self, df_order):

        # inlatur campurile care nu sunt necesare in comanda
        df_comanda_aranjata = df_order.drop(['Cod bare', 'Plic', 'Greutate', 'Observatii', 'SerieClient',
                                                       'RambursNumerar', 'RambursAltTip', 'InstrumentPlata',
                                                       'ValoareInstrumentPlata', 'PlatitorExpeditie', 'LivrareSambata',
                                                       'DeschidereColet', 'Email', 'Continut', 'ValoareDeclarata',
                                                       'Disclaimer',
                                                       'RefExp1', 'RefDest1', 'RefDest2', 'ReferintaFacturare',
                                                       'TaraDest'], axis=1)

        # OBTIN UN SET DE DATE CE REPREZINTA COMANDA EFECTIVA DE PRODUSE
        # facem o copie a setului de date a comenzii pentru a aplica transformari asupra lui
        df_copie_comanda = df_comanda_aranjata.copy()
        # transform cod produs in cod produs din depozit cu doar 9 cifre (scurtez coastele)
        df_copie_comanda.loc[:, 'Cod produs'] = df_order.loc[:, 'Cod produs'].str[0:9]

        # creez un groupby dupa Cod produs si UM si obtin si un total Cantitate
        df_grouped_partial = df_copie_comanda.groupby(['Cod produs']).sum(min_count=1).dropna(axis='columns')
        df_grouped = df_grouped_partial.drop(['RambursContColector'], axis=1)

        # OBTIN UN SET DE DATE CE REPREZINTA DATE ADTIONALE CU PRIVIRE LA CLIENT SI LIVRAREA COMENZII
        # elimin dublicatele pe coloana si coloanele Cantitate
        df_adtional_partial = df_copie_comanda.sort_values('Cod produs').drop(['Cantitate'], axis=1)

        # elimin valorile dublicat de pe toate coloanele
        is_duplicate = df_adtional_partial.apply(pd.Series.duplicated, axis=0)
        df_aditional = df_adtional_partial.where(~is_duplicate, None).dropna(axis='rows', thresh=1)

        # fac join intre cele doua seturi de date pentru a obtine setul de date TCE care sa modeleze fisierul dorit
        df_depozit_neindexat = pd.merge(df_grouped, df_aditional, on='Cod produs')

        # redefinesc indexul pentru setul de date TCE
        df_depozit = df_depozit_neindexat.set_index('Nr. comanda')

        # rescriu UM pentru depozit
        df_copie_depozit = df_depozit.copy()
        df_copie_depozit.loc[:, 'UM'] = df_depozit['UM'].apply(lambda x: x[2:5])

        # redenumesc coloana RambursContColector
        df_copie_depozit.rename(columns={'RambursContColector': 'Ramburs curier'}, inplace=True)

        return df_copie_depozit

    # functie - AWB - prelucreaza  setul de date tip AWB
    def set_date_awb(self, df_order):
        #inlatur campurile care nu sunt necesare AWB
        df_awb_aramjat = df_order.drop(['UM', 'Cod postal', 'Tara', 'Serviciu curier'], axis=1)
        # OBTIN UN SET DE DATE CE REPREZINTA COMANDA EFECTIVA DE PRODUSE
        # facem o copie a setului de date a comenzii pentru a aplica transformari asupra lui
        df_copie_awb_partial = df_awb_aramjat.copy()

        # inlaturam coloanele Cod produs si Denumire care nu ne mai trebuie
        df_copie_awb = df_copie_awb_partial.drop(['Cod produs', 'Denumire'], axis=1)
        # schimb denumirea coloanelor conform AWB Urgent Cargus
        df_copie_awb.rename(columns={'Cantitate': 'Colet', 'Destinatar': 'ClientDest',
                                     'Adresa de livrare': 'AdresaDest',  'Oras': 'OrasDest', 'Judet': 'JudetDest',
                                     'Persoana de contact': 'PersContactDest', 'Telefon': 'TelefonDest'}, inplace=True)
        df_copie_awb.head()

        # creez un groupby dupa Nr. comanda  si obtin si un total Colet
        df_grouped = df_copie_awb.groupby(['Nr. comanda']).sum(min_count=1).dropna(axis='columns')

        # elimin RambursContColector si Greutate
        df_grouped_final = df_grouped.drop(['RambursContColector', 'Greutate'], axis=1)

        # OBTIN UN SET DE DATE CE REPREZINTA DATE ADTIONALE CU PRIVIRE LA CLIENT SI LIVRAREA COMENZII
        # elimin doloana Colet
        df_adtional_partial = df_copie_awb.sort_values('Nr. comanda').drop(['Colet'], axis=1)

        # fac join intre cele doua seturi de date pentru a obtine setul de date TCE care sa modeleze fisierul dorit
        df_awb_neindexat = pd.merge(df_grouped_final, df_adtional_partial, on='Nr. comanda')

        # redefinesc indexul pentru setul de date TCE
        df_awb = ((df_awb_neindexat.set_index('Cod bare')).drop_duplicates())

        df_awb_aranjat = df_awb[['Plic', 'Nr. comanda', 'Colet', 'Greutate', 'ClientDest', 'AdresaDest', 'TaraDest',
                                 'OrasDest', 'JudetDest', 'PersContactDest', 'TelefonDest', 'EmailDest', 'Observatii',
                                 'SerieClient', 'RambursNumerar', 'RambursContColector', 'RambursAltTip',
                                 'InstrumentPlata', 'ValoareInstrumentPlata', 'PlatitorExpeditie', 'LivrareSambata',
                                 'DeschidereColet', 'Email', 'Continut', 'ValoareDeclarata', 'Disclaimer', 'RefExp1',
                                 'RefDest1', 'RefDest2', 'ReferintaFacturare']]


        # transform greutatea din grame in KG
        df_greutate = df_awb_aranjat.copy()
        df_greutate.loc[:, 'Greutate'] = df_awb_aranjat['Greutate'].apply(lambda x: math.ceil(x/1000))


        # completam tabela OBSERVATII
        df_observatii = df_order.copy()
        df_observatii = df_observatii[['Nr. comanda','Cod produs', 'Denumire', 'Cantitate', 'Observatii']]

        df_observatii['Observatii'] = df_observatii.apply(lambda row: str(int((row['Cod produs'])[-3:])/100)+'kg,'
                                                                       if len(row['Cod produs'])==14 else None , axis=1)

        # transform cod produs in cod produs din depozit cu doar 9 cifre (scurtez coastele)
        df_observatii.loc[:, 'Cod produs'] = df_order.loc[:, 'Cod produs'].str[0:9]


        # AICI INCEP SA obtin OBSERVATIILE la o comanda
        df_1073 = df_observatii[df_observatii['Nr. comanda'] == 'nr. 1073']

        df1073_grouped = df_1073.groupby(['Cod produs']).sum(min_count=1).dropna(axis='columns')

        df1073_nearanjat = pd.merge(df1073_grouped, df_1073.drop(['Cantitate'], axis=1), on='Cod produs').drop(['Observatii','Nr. comanda'], axis=1)
        # adaug o coloana goala observatii
        df1073_nearanjat ['Observatii'] = None
        # elimin randurile dublicat
        df1073_aranjat = df1073_nearanjat.drop_duplicates(subset=['Cod produs'], keep= 'first')

        # completam OBSERVATII_1073 manipuland celelate coloane
        df1073_observatii = df1073_aranjat.copy()


        # obtin lista greutatilor baxurilor ce au codul ZTBAX0002
        df1073_ztbax0002= df_1073[df_1073['Cod produs'] == 'ZTBAX0002']
        list_ztbax0002 = df1073_ztbax0002['Observatii'].values.tolist()
        if list_ztbax0002 :
            total_ztbax0002 = '(' +  reduce(lambda x, y: x + y, df1073_ztbax0002['Observatii'].values.tolist())+')'
        else:
            total_ztbax0002 = {}

        #  obtin lista greutatilor baxurilor ce au codul ZTBAX0012
        df1073_ztbax0012 = df_1073[df_1073['Cod produs'] == 'ZTBAX0012']
        list_ztbax0012 = df1073_ztbax0012['Observatii'].values.tolist()
        if  list_ztbax0012  :
             total_ztbax0012 ='(' + reduce(lambda x, y: x + y, df1073_ztbax0012['Observatii'].values.tolist())+')'
        else:
            total_ztbax0012 = {}

        # completam OBSERVATII_1073 manipuland celelate coloane si formatam textul
        df1073_observatii = df1073_aranjat.copy()
        def prelucrare_observatie (x, y, z):
            if x in ['ZTBAX0002']:
                observatie = str(y) +'bax.x ' + z.replace('de', '').replace(' TARANESTI - PALMIERI', ' PALMIERI')\
                    .replace(' - LEVONI', ' LEVONI').replace(' - PALMIERI', '').replace('(cutie  prezentare)', '')\
                    .replace('buc./bax ', '').replace('  ', ' ') + total_ztbax0002+' - '
            elif x in ['ZTBAX0012']:
                observatie = str(y) + 'bax.x ' + z.replace('de', '').replace(' TARANESTI - PALMIERI', ' PALMIERI') \
                    .replace(' - LEVONI', ' LEVONI').replace(' - PALMIERI', '').replace('(cutie  prezentare)', '') \
                    .replace('buc./bax ', '').replace('  ', ' ') + total_ztbax0012 + ' - '
            else:
                observatie =str(y) + 'bax.x ' + z.replace('de', '').replace(' TARANESTI - PALMIERI', ' PALMIERI') \
                    .replace(' - LEVONI', ' LEVONI').replace(' - PALMIERI', '').replace('(cutie  prezentare)', '') \
                    .replace('buc./bax ', '').replace('  ', ' ') + ' - '

            return observatie

        df1073_observatii['Observatii'] = df1073_observatii.apply(lambda row: prelucrare_observatie(row['Cod produs'],row['Cantitate'],row['Denumire']), axis=1)

        #  obtin OBSERVATIA pentru comanda 1073
        total_observatie_1073 = '(' + reduce(lambda x, y: x + y, df1073_observatii['Observatii'].values.tolist()) + ')'
        total_observatie_1073 = total_observatie_1073[: 511]

        # creez un dictionar comanda: observatie
        dict_observatie = {}
        dict_observatie ['nr. 1073'] = total_observatie_1073


        df_dict_observatie = pd.DataFrame(list(dict_observatie.items()), columns=['Nr. comanda', 'Observatii'])

        print(dict_observatie)
        print(df_dict_observatie)
        print(total_observatie_1073)



        return df1073_observatii

    # functie - DEPOZIT -  creeaza un set de date tip depozit care contine toate comenzile din fisierul CSV
    def fisiere_date_depozit(self, df_resursa_comanda):

        # etragem lista de comenzi din fisierul CSV
        lista_comenzi = list(pd.unique(df_resursa_comanda['Nr. comanda']))
        print(lista_comenzi)

        # OBTIN UN SET DE DATE CU TOATE COMENZILE PENTRU DEPOZIT
        # initiez o lista de seturi de date de comenzi
        lista_comenzi_date_depozit = []

        for i in range(len(lista_comenzi)):
            # sparg setul de date din fisierul CSV in comenzi
            df_comanda = df_resursa_comanda[df_resursa_comanda['Nr. comanda'] == str(lista_comenzi[i])]
            # adaug seturi de date de comenzi in lista

            if self.set_date_comanda(df_comanda) is not None:
                lista_comenzi_date_depozit.append(self.set_date_comanda(df_comanda))

        # creez fisiere pentru fiecare set de date de comenzi  din fisierul CSV pentru depozit
        for w in lista_comenzi_date_depozit:
            # creeaz fisier pentru fiecare set de date
            # print(w)
            w.to_excel(str(self.path + str(str(w.iloc[0, 4]))+'_'+str(time.time())) + '.xlsx')

        # df_total = reduce(lambda x, y: x.append(y), lista_comenzi_date_depozit)


    # functie - AWB -  creeaza un set de date tip AWB care contine toate comenzile din fisierul CSV
    def fisier_date_awb(self, df_resursa_awb):


        # creez fisierul AWB corespunzator unei RESURSE COMUNE
        df_awb = self.set_date_awb(df_resursa_awb)
        df_awb.to_excel(str(self.path + 'AWB_'+str(time.time())) + '.xlsx')

    def csv_resursa_to_excel(self):

        # fac o lista cu fisierele CSV tip RESURSA COMUNA din directorul /APP_Orders/csv
        lista_fisiere = self.find('*.csv', self.path)
        print(lista_fisiere)

        for i in range(len(lista_fisiere)):
            # citesc fisierele CSV si creez seturi de date pentru COMENZI TCE si pentru AWB-uri
            df_csv = pd.read_csv(self.path + lista_fisiere[i])

            # apelez scrierea fisierelor pentru COMENZI si AWB
            #self.fisiere_date_depozit(df_csv)
            self.fisier_date_awb(df_csv )


excel_TCE = GetExcelToDepozit('/APP_Orders/csv/')
excel_TCE.csv_resursa_to_excel()

# print(df3.pivot_table(index='Nr. comanda',values=['Cod produs','Cantitate'],aggfunc=sum)) asta aduna coletele

# cand o sa vreau sa diferentiez fiierele prelucraete le mut in alt director si fac doua siruri unul din csv si alt din
# prelucrate si fac diferenta intre ele
