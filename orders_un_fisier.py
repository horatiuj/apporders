import os, fnmatch, openpyxl
import pandas as pd
from functools import reduce


# functia intoarce o lista de fisiere dintr-un anume director
def find(pattern, path):
    result = []
    for root, dirs, files in os.walk(path):
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                result.append(os.path.join(name))
    return result


# functia intoarce un set de date tip depozit pentru o comanda
def set_date_comanda_depozit(df_order):
    # OBTIN UN SET DE DATE CE REPREZINTA COMANDA EFECTIVA DE PRODUSE
    # facem o copie a setului de date a comenzii pentru a aplica transformari asupra lui
    df_copie_comanda = df_order.copy()
    # transform cod produs in cod produs din depozit cu doar 9 cifre (scurtez coastele)
    df_copie_comanda.loc[:, 'Cod produs'] = df_order.loc[:, 'Cod produs'].str[0:9]

    # creez un groupby dupa Cod produs si UM si obtin si un total Cantitate
    df_grouped = df_copie_comanda.groupby(['Cod produs']).sum(min_count=1).dropna(axis='columns')

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

    return df_copie_depozit


# functia creeaza un set de date tip depozit care contine toate comenzile din fisierul CSV
def total_comenzile_set_date_depozit(df_resursa):
    # etragem lista de comenzi din fisierul CSV
    lista_comenzi = list(pd.unique(df_resursa['Nr. comanda']))
    print(lista_comenzi)

    # OBTIN UN SET DE DATE CU TOATE COMENZILE PENTRU DEPOZIT
    # initiez o lista de seturi de date de comenzi
    lista_comenzi_date_depozit = []

    for i in range(len(lista_comenzi)):
        # sparg setul de date din fisierul CSV in comenzi
        df_comanda = df_resursa[df_resursa['Nr. comanda'] == str(lista_comenzi[i])]
        # adaug seturi de date de comenzi in lista
        if set_date_comanda_depozit(df_comanda) is not None:
            lista_comenzi_date_depozit.append(set_date_comanda_depozit(df_comanda))

    # creez un set de date de comenzi cu toate comenzile din fisierul CSV pentru depozit
    df_total = reduce(lambda x, y: x.append(y), lista_comenzi_date_depozit)
    return df_total


def csv_resursa_to_excel_depozit(path):
    # fac o lista cu fisierele CSV din directorul /APP_Orders/csv
    lista_fisiere = find('*.csv', path)
    print(lista_fisiere)

    for i in range(len(lista_fisiere)):
        # citesc fisierele CSV si creez seturi de date pentru fiacare dintre ele
        df_csv = pd.read_csv(path + lista_fisiere[i])

        # adun toate seturile de comenzi intr-un singur set cu care creeaz un fisier
        df_final = total_comenzile_set_date_depozit(df_csv)

        # creez fisierul EXCEL de comenzi pentru TCE
        df_final.to_excel(str(path + str(lista_fisiere[i][:-4]) + '.xlsx'))


csv_resursa_to_excel_depozit('/APP_Orders/csv/')

# print(df3.pivot_table(index='Nr. comanda',values=['Cod produs','Cantitate'],aggfunc=sum)) asta aduna coletele
