"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
Name...: dba_busca_usuarios.py
Purpose: Este script busca los usuarios de base Oracle y los filatra, para poder obtener
         los usuarios que fueron creados, fuera de los schemas originales del producto.
Notes..: Los usuarios default fueron tomados de la documentación de Oracle en:
         https://docs.oracle.com/cd/E26401_01/doc.122/e22952/T156458T659606.htm
History: 01/04/2020 Created

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
import funcs as f
import pandas as pd
import datetime
import threading
import connections as c


# Seteo pandas para que muestre todas las filas y todas las columnas del dataframe.
#pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

def reportDbUsers(self, completo=False, envia_sftp=False, args=()):
    print(" "); print("+ REPORT DB ORACLE")
    print("  completo =", completo, "; envia_sftp =", envia_sftp)
    print("  args =", args)

    """
    # Default Users according Oracle documentation.
    users_type_1 = ['SYS', 'SYSTEM', 'DBSNMP', 'SYSMAN', 'MGMT_VIEW']
    users_type_2 = ['SCOTT', 'SSOSDK']
    users_type_3 = ['JUNK_PS', 'MDSYS', 'ODM_MTR', 'OLAPSYS', 'ORDPLUGINS', 'ORDSYS', 'OUTLN', 'OWAPUB',
                    'MGDSYS', 'PORTAL30_DEMO', 'PORTAL30_PUBLIC', 'PORTAL30_PS', 'PORTAL30_SSO_PUBLIC']
    users_type_4 = ['PORTAL30', 'PORTAL30_SSO', 'CTXSYS', 'EDWREP', 'ODM']
    users_type_5 = ['APPLSYSPUB', 'APPLSYS', 'APPS', 'APPS_NE', 'APPS_mrc', 'AD_MONITOR', 'EM_MONITOR']
    users_type_6 = ['ABM', 'AHL', 'AHM', 'AK', 'ALR', 'AMF', 'AMS', 'AMV', 'AMW', 'AP', 'AR', 'ASF', 'ASG', 'ASL', 'ASN',
                    'ASO', 'ASP', 'AST', 'AX', 'AZ', 'BEN', 'BIC', 'BIL', 'BIM', 'BIS', 'BIV','BIX', 'BNE', 'BOM', 'BSC',
                    'CCT', 'CE', 'CLN', 'CN', 'CRP', 'CS', 'CSC', 'CSD', 'CSE', 'CSF', 'CSI', 'CSL', 'CSM', 'CSP', 'CSR',
                    'CSS', 'CUA', 'CUE', 'CUF', 'CUG', 'CUI', 'CUN', 'CUP', 'CUS', 'CZ', 'DDD', 'DDR', 'DNA', 'DOM', 'DPP',
                    'EAA', 'EAM', 'EC', 'ECX', 'EDR', 'EGO', 'ENG', 'ENI', 'EVM', 'FA', 'FEM', 'FII', 'FLM', 'FPA', 'FPT',
                    'FRM', 'FTE', 'FTP', 'FUN', 'FV', 'GCS', 'GHG', 'GL', 'GMA', 'GMD', 'GME', 'GMF', 'GMI', 'GML', 'GMO',
                    'GMP', 'GMS', 'GR', 'HR', 'HRI', 'HXC', 'HXT', 'IA', 'IBA', 'IBC', 'IBE', 'IBP', 'IBU', 'IBW', 'IBY',
                    'ICX', 'IEB', 'IEC', 'IEM', 'IEO', 'IES', 'IEU', 'IEX', 'IGC', 'IGF', 'IGI', 'IGS', 'IGW', 'IMC', 'IMT',
                    'INL', 'INV', 'IPA', 'IPD', 'IPM', 'ISC', 'ITA', 'ITG', 'IZU', 'JA', 'JE', 'JG', 'JL', 'JMF', 'JTF',
                    'JTM', 'JTS', 'LNS', 'ME', 'MFG', 'MRP', 'MSC', 'MSD', 'MSO', 'MSR', 'MST', 'MTH', 'MWA', 'OE', 'OKB',
                    'OKC', 'OKE', 'OKI', 'OKL', 'OKO', 'OKR', 'OKS', 'OKX', 'ONT', 'OPI', 'OSM', 'OTA', 'OZF', 'OZP', 'OZS',
                    'PA', 'PFT', 'PJI', 'PJM', 'PMI', 'PN', 'PO', 'POA', 'POM', 'PON', 'POS', 'PRP', 'PSA', 'PSB', 'PSP',
                    'PV', 'QA', 'QOT', 'QP', 'QPR', 'QRM', 'RG', 'RHX', 'RLA', 'RLM', 'RRS', 'SSP', 'VEA', 'VEH', 'WIP',
                    'WMS', 'WPS', 'WSH', 'WSM', 'XDO', 'XDP', 'XLA', 'XLE', 'XNB', 'XNC', 'XNI', 'XNM', 'XNP', 'XNS', 'XTR',
                    'ZFA', 'ZPB', 'ZSA', 'ZX']
    users_type_7 = ['CLL', 'NEWRELIC', 'NAGIOS', 'BOLINF', 'OAS_APP', 'QLICKVIEW', 'BACKO_APP', 'PIGEON_APP', 'FLOWWE_APP'
                    'IMPERVA']

    # Dejo todos los usuarios default en la variable def_users.
    #def_users = users_type_1 + users_type_2 + users_type_3 + users_type_4 + users_type_5 + users_type_6 + users_type_7

    def_users = ['DATA_RO_APP', 'DATALAKE', 'IMPERVA', 'MGMT_VIEW', 'SYSMAN', 'CFA_REP_APP', 'REVENUE_APP', 'CFA_BACKO_APP',
                 'APP_BCHIP', 'XXISV', 'BOLINFTMP', 'FLOWWE_APP', 'PIGEON_APP', 'CTRACE_APP', 'BACKO_APP', 'QLICKVIEW',
                 'FACTURASMV', 'ROBOT_FACTURAS', 'OAS_APP', 'INTERFACEGL', 'BOLINF', 'NAGIOS', 'NEWRELIC', 'CLL', 'MGDSYS',
                 'INL', 'DDR', 'QPR', 'MTH', 'DPP', 'RRS', 'IZU', 'OLAPSYS', 'IBW', 'IPM', 'PFT', 'FTP', 'ITA', 'JMF', 'DNA',
                 'GMO', 'LNS', 'ZPB', 'FPA', 'IA', 'GCS', 'AMW', 'XLE', 'ZX', 'XDO', 'FUN', 'MST', 'ASN', 'ODM_MTR', 'ODM',
                 'SSOSDK', 'PRP', 'EDR', 'ZSA', 'ZFA', 'XNB', 'CLN', 'PJI', 'EGO', 'DOM', 'CSM', 'QOT', 'DDD', 'AMF', 'IBC',
                 'OKL', 'PON', 'QRM', 'BNE', 'DBSNMP', 'JTM', 'JTS', 'AHL', 'OKB', 'XNI', 'IMC', 'OKO', 'ASP', 'AHM', 'POS',
                 'OKI', 'IEC', 'IMT', 'BIV', 'CSI', 'CSL', 'CUG', 'CSE', 'IPD', 'ENI', 'ITG', 'OKR', 'MSR', 'EAM', 'IGI',
                 'CUE', 'FTE', 'ASL', 'PV', 'APPS', 'PSA', 'IGC', 'IGS', 'CSR', 'IEB', 'IGF', 'MWA', 'PSP', 'FV', 'WSM', 'OZP',
                 'OZS', 'PSB', 'IGW', 'IEU', 'IEM', 'OKE', 'ECX', 'GMS', 'OKX', 'ASO', 'CSP', 'OZF', 'CUA', 'IPA', 'ASG', 'IEX',
                 'WSH', 'WMS', 'MSO', 'WPS', 'ONT', 'QP', 'CUF', 'MSD', 'IBY', 'POA', 'OPI', 'ISC', 'HRI', 'FII', 'IBU', 'IBE',
                 'CSS', 'AST', 'CCT', 'FPT', 'IBP', 'IES', 'IBA', 'XNS', 'AMV', 'BIL', 'XNP', 'XNM', 'BIM', 'XNC', 'XDP', 'ME',
                 'OKC', 'OKS', 'CSC', 'BIC', 'CSD', 'ASF', 'AMS', 'CSF', 'IEO', 'CUP', 'CUI', 'CUS', 'BIX', 'JTF', 'CUN', 'GR',
                 'PMI', 'GMD', 'GME', 'GMF', 'GMI', 'GML', 'GMP', 'EC', 'GMA', 'JE', 'JL', 'CS', 'CE', 'JA', 'JG', 'CRP', 'CZ',
                 'PJM', 'FLM', 'MSC', 'WIP', 'RHX', 'XTR', 'INV', 'MFG', 'CN', 'MRP', 'PO', 'BOM', 'ENG', 'FEM', 'AR', 'OE',
                 'OSM', 'PA', 'AP', 'EVM', 'RLM', 'HXC', 'BSC', 'FRM', 'POM', 'EAA', 'ABM', 'VEA', 'PN', 'BIS', 'QA', 'ICX',
                 'AZ', 'HXT', 'OTA', 'SSP', 'VEH', 'BEN', 'RLA', 'AK', 'FA', 'RG', 'GL', 'XLA', 'AX', 'HR', 'APPLSYS', 'ALR',
                 'APPLSYSPUB', 'OWAPUB', 'CTXSYS', 'MDSYS', 'ORDPLUGINS', 'ORDSYS', 'OUTLN', 'SYSTEM', 'SYS']

    print("Default Users: ", def_users)
    print(" ")
    """
    app_users = ['DATA_RO_APP', 'DATALAKE', 'IMPERVA', 'MGMT_VIEW', 'CFA_REP_APP', 'REVENUE_APP', 'CFA_BACKO_APP',
                 'APP_BCHIP', 'XXISV', 'BOLINFTMP', 'FLOWWE_APP', 'PIGEON_APP', 'CTRACE_APP', 'BACKO_APP', 'QLICKVIEW',
                 'FACTURASMV', 'ROBOT_FACTURAS', 'OAS_APP', 'INTERFACEGL', 'NAGIOS', 'NEWRELIC', 'ORACLE_OCM', 'ORACONS',
                 'ANKAA_DESPEGAR', 'MASTERDATA', 'RBT_AUTOANYWHERE_APP', 'CONSULTADBA','CONSULTAS']

    def_users = ['INL', 'DDR', 'QPR', 'MTH', 'DPP', 'RRS', 'IZU', 'OLAPSYS', 'IBW', 'IPM', 'PFT', 'FTP', 'ITA', 'JMF', 'DNA',
                 'GMO', 'LNS', 'ZPB', 'FPA', 'IA', 'GCS', 'AMW', 'XLE', 'ZX', 'XDO', 'FUN', 'MST', 'ASN', 'ODM_MTR', 'ODM',
                 'SSOSDK', 'PRP', 'EDR', 'ZSA', 'ZFA', 'XNB', 'CLN', 'PJI', 'EGO', 'DOM', 'CSM', 'QOT', 'DDD', 'AMF', 'IBC',
                 'OKL', 'PON', 'QRM', 'BNE', 'DBSNMP', 'JTM', 'JTS', 'AHL', 'OKB', 'XNI', 'IMC', 'OKO', 'ASP', 'AHM', 'POS',
                 'OKI', 'IEC', 'IMT', 'BIV', 'CSI', 'CSL', 'CUG', 'CSE', 'IPD', 'ENI', 'ITG', 'OKR', 'MSR', 'EAM', 'IGI',
                 'CUE', 'FTE', 'ASL', 'PV', 'APPS', 'PSA', 'IGC', 'IGS', 'CSR', 'IEB', 'IGF', 'MWA', 'PSP', 'FV', 'WSM', 'OZP',
                 'OZS', 'PSB', 'IGW', 'IEU', 'IEM', 'OKE', 'ECX', 'GMS', 'OKX', 'ASO', 'CSP', 'OZF', 'CUA', 'IPA', 'ASG', 'IEX',
                 'WSH', 'WMS', 'MSO', 'WPS', 'ONT', 'QP', 'CUF', 'MSD', 'IBY', 'POA', 'OPI', 'ISC', 'HRI', 'FII', 'IBU', 'IBE',
                 'CSS', 'AST', 'CCT', 'FPT', 'IBP', 'IES', 'IBA', 'XNS', 'AMV', 'BIL', 'XNP', 'XNM', 'BIM', 'XNC', 'XDP', 'ME',
                 'OKC', 'OKS', 'CSC', 'BIC', 'CSD', 'ASF', 'AMS', 'CSF', 'IEO', 'CUP', 'CUI', 'CUS', 'BIX', 'JTF', 'CUN', 'GR',
                 'PMI', 'GMD', 'GME', 'GMF', 'GMI', 'GML', 'GMP', 'EC', 'GMA', 'JE', 'JL', 'CS', 'CE', 'JA', 'JG', 'CRP', 'CZ',
                 'PJM', 'FLM', 'MSC', 'WIP', 'RHX', 'XTR', 'INV', 'MFG', 'CN', 'MRP', 'PO', 'BOM', 'ENG', 'FEM', 'AR', 'OE',
                 'OSM', 'PA', 'AP', 'EVM', 'RLM', 'HXC', 'BSC', 'FRM', 'POM', 'EAA', 'ABM', 'VEA', 'PN', 'BIS', 'QA', 'ICX',
                 'AZ', 'HXT', 'OTA', 'SSP', 'VEH', 'BEN', 'RLA', 'AK', 'FA', 'RG', 'GL', 'XLA', 'AX', 'HR', 'APPLSYS', 'ALR',
                 'SYSMAN', 'BOLINF', 'CLL', 'MGDSYS', 'APPLSYSPUB', 'OWAPUB', 'CTXSYS', 'MDSYS', 'ORDPLUGINS', 'ORDSYS',
                 'OUTLN', 'SYSTEM', 'SYS', 'SCOTT', 'DIP', 'XDB', 'ANONYMOUS', 'SI_INFORMTN_SCHEMA', 'MDDATA', 'EM_MONITOR',
                 'XS$NULL', 'AD_MONITOR', 'PORTAL30_DEMO', 'PORTAL30_PUBLIC', 'PORTAL30_PS', 'PORTAL30_SSO_PUBLIC',
                 'JUNK_PS', 'PORTAL30_SSO', 'EDWREP', 'APPS_mrc', 'GHG', 'PORTAL30', 'APPS_NE', 'APPQOSSYS',
                 'SPATIAL_WFS_ADMIN_USR', 'SPATIAL_CSW_ADMIN_USR', 'ORDDATA']

    # print("+ HiloDb")
    #

    start = datetime.datetime.now()

    # Hago la conexión a Oracle.
    oracle = c.getOracle()
    df = oracle.getDB()
    print(" "); print("df:"); print(df)

    # Agrego la columna DEF_USER con 'Y' cuando el valor del UserName está en la lista de def_users.
    df['USER_TYPE'] = df['USERNAME'].apply(lambda x: 'Sistema' if x in def_users else ("Aplicativos" if x in app_users else "Nominal"))
    print(" "); print("df (con nueva col User_Type):"); print(df)

    # ---------------------------------------------------------------------------------------------
    # Voy a buscar en la planilla de RRHH la fecha de baja.
    # ---------------------------------------------------------------------------------------------
    # df_csv = c.getCSV()
    df_csv = args[0]       # es el df_rrhh del parámetro

    # Genero la columna USER_NAME con un substr(email_address) hasta el "@".
    df_csv["USER_NAME"] = df_csv["EMAIL_ADDRESS"].apply(lambda x: x[0:x.find("@")].upper() if not pd.isna(x) else x)
    print("- Abro archivo csv")
    print(" "); print("df_csv = "); print(df_csv)

    # ---------------------------------------------------------------------------------------------
    # Esto es maravilloso: agrega 3 columnas a df, matcheando el User_Name de ambos dataframes.
    # ---------------------------------------------------------------------------------------------
    df.set_index('USERNAME', inplace=True, drop=False)
    df_csv.set_index('USER_NAME', inplace=True, drop=False)

    df["CSV_USER"] = df.join(df_csv, lsuffix="df")["USER_NAME"]
    df["CSV_END_DATE"] = df.join(df_csv, lsuffix="df")["END_DATE"]
    df["EMAIL_ADDRESS"] = df.join(df_csv, lsuffix="df")["EMAIL_ADDRESS"]
    df["CREATION_DATE"] = df.join(df_csv, lsuffix="df")["CREATED"]
    df["AD_SAMACCOUNTNAME"] = ""
    df["AD_LASTLOGON"] = ""
    df["AD_USERACCOUNTCONTROL"] = ""
    df["AD_CUENTAS"] = ""

    df["CREATION_DATE"] = df["CREATION_DATE"].apply(lambda x: f.formateaFecha(str(x), '%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M'))

    df.reset_index(drop=True, inplace=True)
    df_csv.reset_index(drop=True, inplace=True)

    print(" "); print("df completo = "); print(df)

    # ---------------------------------------------------------------------------------------------
    # Recupero datos del AD.
    # ---------------------------------------------------------------------------------------------
    # self.prog.max(len(df))
    print("+ Recupero del AD")
    ad_samaccount = c.getAD()

    try:
        for i in range(0, len(df)):
            if not pd.isna(df.loc[i, "EMAIL_ADDRESS"]):
                # self.prog.adv(i)
                ad = ad_samaccount.get_samAccountName(df.loc[i, "EMAIL_ADDRESS"])
                df.loc[i, "AD_SAMACCOUNTNAME"] = ad[0]  # samaccountname
                df.loc[i, "AD_LASTLOGON"] = ad[1]  # lastlogontimestamp
                df.loc[i, "AD_USERACCOUNTCONTROL"] = ad[2]  # useraccountcontrol
                df.loc[i, "AD_CUENTAS"] = ad[3]  # cant cuentas de AD
    except Exception as err:
        print(err)
    print("- Recupero del AD")

    # Agrego la columna de Comentarios.
    df.loc[(pd.isna(df["CSV_END_DATE"]) == False) & (df["ACCOUNT_STATUS"] == "OPEN"), "COMMENTS"] = "Dar de Baja"

    # Cambio el orden de las columnas (para mandar el df al QTableWidget de pantalla).
    df = df[["USERNAME", "ACCOUNT_STATUS", "USER_TYPE", "LOCK_DATE", "PROFILE", "CREATION_DATE", "CSV_USER", "CSV_END_DATE",
             "AD_USERACCOUNTCONTROL", "AD_SAMACCOUNTNAME", "AD_LASTLOGON", "EMAIL_ADDRESS", "AD_CUENTAS", "COMMENTS"]]
    print(" "); print("df final: "); print(df)

    df.fillna("", inplace=True)

    # ----------------------------------------------------------------------
    # Filtro por los usuarios que no son del sistema y además están activos.
    # ----------------------------------------------------------------------
    df2 = df[(df["USER_TYPE"] == "NOMINAL") & (df["ACCOUNT_STATUS"] == "OPEN")]
    print(" "); print("df2 filtrado: "); print(df2)
    print("df2.shape = ", df2.shape)

    # Si el df2 no tiene datos, entonces la planilla debe informar "Sin Datos" (redefino el df2)
    if df2.shape[0] == 0:
        df2_cols = df2.columns
        df2 = pd.DataFrame(columns=df2_cols)
        df2.loc[0, df.columns[0]] = "Sin Datos"

    # ----------------------------------------------------------------------
    # Genero el Reporte
    # ----------------------------------------------------------------------
    print("+ Genero Reporte_Oracle_DB")
    if completo:
        f.quick_excel(df, "Reporte_Oracle_DB_Users (Full)", final=True, put_sftp=envia_sftp)
    else:
        f.quick_excel(df2, "Reporte_Oracle_DB_Users", final=True, put_sftp=envia_sftp)
    print("- Genero Reporte_Oracle_DB")

    print("Processing elapsed time {}".format(datetime.datetime.now() - start))  # esto muestra el tiempo de ejecución.

    print(" "); print("- REPORT DB ORACLE")
