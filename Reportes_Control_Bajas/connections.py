# + estas importaciones son para el acceso a GSuite: para que funcionen estas importaciones
# hay que instalar primero el módulo googleapi (instalás googleapi y todo lo demás queda ok).
from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build             # para instalar esta librería: pip install GoogleApi
# from google-api-python-client import googleapiclient
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
# - estas importaciones son para el acceso a GSuite.
import datetime
import os
from ldap3 import Server, Connection, ALL, SUBTREE
from configparser import ConfigParser
import cx_Oracle        # el package que hay que instalar es cx-Oracle (con guión alto); lo instalé con guión bajo y anduvo ok.
import pandas as pd
import numpy as np
import mysql.connector      # se debe instalar con esto: pip install mysql-connector-python
import funcs as f
import sqlite3 as db
import re


# pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


class getOracle():
    def __init__(self):
        cparser = ConfigParser()  # crea el objeto ConfigParser
        cparser.read("params/users.conf")

        # Oracle Constants.
        c_ora_host = cparser.get("ORACLE", "Host")
        c_ora_sid = cparser.get("ORACLE", "SID")
        c_ora_user_name = cparser.get("ORACLE", "User Name")
        c_ora_password = f.dec(cparser.get("ORACLE", "Password"))

        # Conexión a Oracle.
        print("+ Conexión a Oracle")
        try:
            self.conn = cx_Oracle.connect(c_ora_user_name, c_ora_password, c_ora_host + "/" + c_ora_sid)
            self.cur = self.conn.cursor()
            print("Conexión ORACLE exitosa")
        except Exception as err:
            print("Error en la conexión a ORACLE; Error: ", err)
            return
        print("- Conexión a Oracle")

        # Hago los seteos de la sesión.
        self.conn.current_schema = "APPS"

    def get_actives(self, p_email_address=None, p_full_name=None):
        sql = "SELECT NVL(fu.user_name, ' ') ora_user_name" \
              "      ,CASE " \
              "         WHEN fu.user_id IS NULL THEN 'UNASSIGNED' " \
              "         WHEN (TO_CHAR(fu.start_date, 'MM/DD/YYYY HH24:MI') = TO_CHAR(TO_DATE('1','j'), 'MM/DD/YYYY HH24:MI') " \
              "          AND TO_CHAR(fu.end_date, 'MM/DD/YYYY HH24:MI') = TO_CHAR(TO_DATE('1','j'), 'MM/DD/YYYY HH24:MI')) THEN 'PENDING' " \
              "         WHEN fu.encrypted_user_password = 'INVALID' THEN 'LOCKED' " \
              "         WHEN fu.encrypted_user_password = 'EXTERNAL' THEN 'EXTERNAL' " \
              "         WHEN (fu.start_date IS NOT NULL AND fu.start_date <= SYSDATE) " \
              "          AND (fu.end_date IS NULL OR fu.end_date > SYSDATE) THEN 'ACTIVE' " \
              "         ELSE 'INACTIVE' " \
              "       END ora_status " \
              "      ,NVL(fu.email_address,pp.email_address) email_address " \
              "      ,pp.full_name ora_full_name" \
              "      ,(SELECT MAX(pps.date_start) FROM per_periods_of_service pps WHERE pps.person_id = pp.person_id) ora_start_date " \
              "      ,trunc(fu.end_date) ora_end_date " \
              "      ,fu.last_logon_date ora_last_logon_date " \
              "      ,TRUNC(SYSDATE - NVL(fu.last_logon_date,(SELECT MAX(pps.date_start) " \
              "                                                 FROM per_periods_of_service pps " \
              "                                                WHERE pps.person_id = pp.person_id))) ora_elapsed_days " \
              "      ,(SELECT NAME FROM gl_ledgers WHERE ledger_id = pa.set_of_books_id) ora_country_code " \
              "      ,(SELECT sup.full_name FROM per_people_f sup WHERE sup.person_id = pa.supervisor_id" \
              "           AND SYSDATE BETWEEN sup.effective_start_date AND sup.effective_end_date) ora_supervisor_name" \
              "      ,(SELECT ss.full_name FROM per_people_f ss " \
              "         WHERE SYSDATE BETWEEN ss.effective_start_date AND ss.effective_end_date" \
              "           AND ss.person_id = (SELECT supervisor_id FROM per_assignments_f ps " \
              "                                WHERE SYSDATE BETWEEN ps.effective_start_date AND ps.effective_end_date" \
              "                                  AND person_id = pa.supervisor_id)) ora_super_supervisor " \
              "      ,pp.employee_number ora_employee_number " \
              "      ,(SELECT concatenated_segments FROM gl_code_combinations_kfv WHERE code_combination_id = pa.default_code_comb_id) adi" \
              "      ,(SELECT TO_CHAR(COUNT(1)) FROM fnd_user_resp_groups_direct furg " \
              "         WHERE furg.user_id = fu.user_id " \
              "           AND NVL(furg.end_date,SYSDATE) > TRUNC(SYSDATE) ) k_resp, 'ORACLE' origen " \
              "  FROM (SELECT * FROM per_assignments_f WHERE SYSDATE BETWEEN effective_start_date AND effective_end_date) pa" \
              "      ,(SELECT * FROM per_people_f WHERE SYSDATE BETWEEN effective_start_date AND effective_end_date) pp" \
              "      ,fnd_user           fu" \
              " WHERE 1=1 " \
              "   AND NVL(fu.end_date,SYSDATE) > TRUNC(SYSDATE)" \
              "   AND pp.person_id(+) = fu.employee_id" \
              "   AND pa.person_id(+) = fu.employee_id" \
              "   AND fu.employee_id IS NOT NULL" \
              "   AND NOT EXISTS (SELECT 1 FROM fnd_lookup_values" \
              "                    WHERE lookup_type = 'XX_RESERVED_USERS'" \
              "                      AND language = 'US'" \
              "                      AND lookup_code = fu.user_name" \
              "                      AND enabled_flag = 'Y'" \
              "                      AND NVL(end_date_active,SYSDATE) > TRUNC(SYSDATE) )" \
              "   AND 1=1" \
              "UNION  " \
              "SELECT NVL(fu.user_name, ' ') ora_user_name" \
              "      ,CASE " \
              "         WHEN fu.user_id IS NULL THEN 'UNASSIGNED' " \
              "         WHEN (TO_CHAR(fu.start_date, 'MM/DD/YYYY HH24:MI') = TO_CHAR(TO_DATE('1','j'), 'MM/DD/YYYY HH24:MI') " \
              "          AND TO_CHAR(fu.end_date, 'MM/DD/YYYY HH24:MI') = TO_CHAR(TO_DATE('1','j'), 'MM/DD/YYYY HH24:MI')) THEN 'PENDING' " \
              "         WHEN fu.encrypted_user_password = 'INVALID' THEN 'LOCKED' " \
              "         WHEN fu.encrypted_user_password = 'EXTERNAL' THEN 'EXTERNAL' " \
              "         WHEN (fu.start_date IS NOT NULL AND fu.start_date <= SYSDATE) " \
              "          AND (fu.end_date IS NULL OR fu.end_date > SYSDATE) THEN 'ACTIVE' " \
              "         ELSE 'INACTIVE' " \
              "       END ora_status " \
              "      ,fu.email_address " \
              "      ,NULL ora_full_name" \
              "      ,fu.start_date  ora_start_date " \
              "      ,trunc(fu.end_date) ora_end_date " \
              "      ,fu.last_logon_date ora_last_logon_date " \
              "      ,TRUNC(SYSDATE - NVL(fu.last_logon_date,fu.start_date)) ora_elapsed_days " \
              "      ,NULL  ora_country_code " \
              "      ,NULL  ora_supervisor_name" \
              "      ,NULL  ora_super_supervisor " \
              "      ,NULL  ora_employee_number " \
              "      ,NULL  adi" \
              "      ,(SELECT TO_CHAR(COUNT(1)) FROM fnd_user_resp_groups_direct furg " \
              "         WHERE furg.user_id = fu.user_id " \
              "           AND NVL(furg.end_date,SYSDATE) > TRUNC(SYSDATE) ) k_resp, 'ORACLE' origen " \
              "  FROM fnd_user           fu" \
              " WHERE 1=1" \
              "   AND NVL(fu.end_date,SYSDATE) > TRUNC(SYSDATE) " \
              "   AND fu.employee_id IS NULL" \
              "   AND NOT EXISTS (SELECT 1 FROM fnd_lookup_values" \
              "                    WHERE lookup_type = 'XX_RESERVED_USERS'" \
              "                      AND language = 'US'" \
              "                      AND lookup_code = fu.user_name" \
              "                      AND enabled_flag = 'Y'" \
              "                      AND NVL(end_date_active,SYSDATE) > TRUNC(SYSDATE) )" \
              "   AND 1=1"
        print(sql)
        # self.cur.execute(sql)

        # Creo el df con datos de Oracle.
        df_ora = pd.read_sql(sql, self.conn)

        # Cierro la conexión.
        self.conn.close()

        # Convierto datos.
        cols = ["ORA_START_DATE", "ORA_END_DATE", "ORA_LAST_LOGON_DATE"]
        for c in range(0, len(cols)):
            df_ora[cols[c]] = df_ora[cols[c]].apply(lambda x: f.formateaFecha(str(x), '%Y-%m-%d %H:%M:%S', '%d/%m/%Y'))     # deja la fecha como str.

        df_ora["EMAIL_ADDRESS"] = df_ora["EMAIL_ADDRESS"].str.lower()

        # Limpio nulos.
        df_ora["EMAIL_ADDRESS"].fillna("", inplace=True)

        print(" "); print("df_ora: "); print(df_ora)
        f.quick_excel(df_ora, "Rep_Oracle")

        return df_ora

    def getDB(self):
        sql = "SELECT username, account_status, lock_date, profile, created, user_id FROM dba_users"
        print(sql)
        # self.cur.execute(sql)

        # Creo el df con datos de Oracle.
        df_ora = pd.read_sql(sql, self.conn)

        # Cierro la conexión.
        self.conn.close()

        return df_ora

    def getApplUsers(self):
        sql = "SELECT fu.user_name, DECODE(UPPER(flv.tag), 'S', 'Sistema', DECODE(UPPER(flv.tag), 'X', 'Aplicativos', DECODE(UPPER(flv.tag), 'S-A', 'Administrador','Nominal'))) user_type " \
              "      ,fu.start_date, NVL(fu.email_address,pp.email_address) email_address, pp.full_name " \
              "      ,(SELECT DISTINCT (select gl.name from gl_ledgers gl where gl.ledger_id = pa.set_of_books_id) " \
              "          FROM per_assignments_f pa WHERE pa.person_id = fu.employee_id " \
              "           AND SYSDATE between effective_start_date AND effective_end_date) country " \
              "  FROM (SELECT person_id, email_address, full_name FROM per_people_f pp " \
              "         WHERE SYSDATE BETWEEN effective_start_date AND effective_end_date) pp" \
              "      ,(SELECT lookup_code, tag FROM fnd_lookup_values " \
              "         WHERE lookup_type = 'XX_RESERVED_USERS' AND language = 'US' AND enabled_flag = 'Y' " \
              "           AND NVL(end_date_active, SYSDATE) > TRUNC(SYSDATE)) flv" \
              "      ,fnd_user fu " \
              " WHERE fu.user_name = trim(flv.lookup_code(+))" \
              "   AND NVL(fu.end_date,SYSDATE) > TRUNC(SYSDATE)" \
              "   AND pp.person_id(+) = fu.employee_id "
        print(sql)
        # self.cur.execute(sql)

        # Creo el df con datos de Oracle.
        df_ora = pd.read_sql(sql, self.conn)

        # Cierro la conexión.
        self.conn.close()

        df_ora["START_DATE"] = df_ora["START_DATE"].apply(lambda x: f.formateaFecha(str(x), '%Y-%m-%d %H:%M:%S', '%d/%m/%Y') if not pd.isna(x) else x)

        return df_ora


    def get_oracle(self, p_sql):
        self.cur.execute(p_sql)
        df_ora = pd.read_sql(p_sql, self.conn)
        self.conn.close()

        return df_ora



class getOpecus():
    def __init__(self):
        cparser = ConfigParser()  # crea el objeto ConfigParser
        cparser.read("params/users.conf")

        c_opq_host = cparser.get("OPECUS", "Host")
        c_opq_port = cparser.get("OPECUS", "Port")
        c_opq_database = cparser.get("OPECUS", "Database")
        c_opq_user_name = cparser.get("OPECUS", "User Name")
        c_opq_password = f.dec(cparser.get("OPECUS", "Password"))

        print("+ Conexión a Opecus")
        try:
            self.conn_opq = mysql.connector.connect(host=c_opq_host,
                                                    port=c_opq_port,
                                                    database=c_opq_database,
                                                    user=c_opq_user_name,
                                                    password=c_opq_password)
            print("Conexión OPECUS exitosa")
        except Exception as err:
            print("ERROR en la conexión a OPECUS; ", err)
            return
        print("- Conexión a Opecus")

        """
        if self.conn.is_connected() == False:
            print('ERROR al conectarse')
            self.conn.close()
            quit()
        else:
            print("  Conexión exitosa")
        """

    def get_actives(self):
        sql = "select email, concat(lastname,', ',name) full_name, username, nombre, country_code, admission_date " \
              "      ,null discharge_date, status, supervisor_complete_name, if(zone_pci=1, 'Si', '  ') zone_pci, 'OPECUS' origen " \
              "  from idm_view " \
              " where status = 'Activado'"
        print(sql)
        self.cur = self.conn_opq.cursor()
        print("+ Ejecuta sql")
        self.cur.execute(sql)
        print("- Ejecuta sql")
        records = self.cur.fetchall()

        # Genero el df
        df_opq = pd.DataFrame(records, columns=["EMAIL_ADDRESS", "OPQ_FULL_NAME", "OPQ_USERNAME", "OPQ_CONSULTORA", "OPQ_COUNTRY_CODE",
                                                "OPQ_START_DATE", "OPQ_END_DATE", "OPQ_STATUS", "OPQ_SUPERVISOR_NAME", "PCI", "ORIGEN"])
        # Cierro la conexión.
        # self.conn_opq.close()

        # Elimino todas las filas que tienen el mail vacío o empiezan con "sinmail".
        # df_opq["EMAIL_ADDRESS"] = df_opq["EMAIL_ADDRESS"].apply(lambda x: pd.isna(x) == False and x[0:x.find("@")].upper() != "SINMAIL")

        # Convierto datos.
        df_opq["OPQ_END_DATE"] = df_opq["OPQ_END_DATE"].apply(lambda x: f.formateaFecha(str(x), '%Y-%m-%d', '%d/%m/%Y'))
        df_opq["OPQ_START_DATE"] = df_opq["OPQ_START_DATE"].apply(lambda x: f.formateaFecha(str(x), '%Y-%m-%d', '%d/%m/%Y'))
        df_opq["EMAIL_ADDRESS"] = df_opq["EMAIL_ADDRESS"].str.lower()

        return df_opq


    def get_inactives(self):
        sql = "select email, concat(lastname,', ',name) full_name, username, nombre, country_code, admission_date " \
              "      ,discharge_date, status, supervisor_complete_name, if(zone_pci=1, 'Si', '  ') zone_pci, 'OPECUS' origen " \
              "  from idm_view " \
              " where status = 'Desactivado'"
        print(sql)
        self.cur = self.conn_opq.cursor()
        print("+ Ejecuta sql")
        self.cur.execute(sql)
        print("- Ejecuta sql")
        records = self.cur.fetchall()

        # Genero el df
        df_opq = pd.DataFrame(records, columns=["EMAIL_ADDRESS", "OPQ_FULL_NAME", "OPQ_USERNAME", "OPQ_CONSULTORA", "OPQ_COUNTRY_CODE",
                                                "OPQ_START_DATE", "OPQ_END_DATE", "OPQ_STATUS", "OPQ_SUPERVISOR_NAME", "PCI", "ORIGEN"])
        # Cierro la conexión.
        # self.conn_opq.close()

        # Elimino todas las filas que tienen el mail vacío o empiezan con "sinmail".
        # df_opq["EMAIL_ADDRESS"] = df_opq["EMAIL_ADDRESS"].apply(lambda x: pd.isna(x) == False and x[0:x.find("@")].upper() != "SINMAIL")

        # Convierto datos.
        df_opq["OPQ_END_DATE"] = df_opq["OPQ_END_DATE"].apply(lambda x: f.formateaFecha(str(x), '%Y-%m-%d', '%d/%m/%Y'))
        df_opq["OPQ_START_DATE"] = df_opq["OPQ_START_DATE"].apply(lambda x: f.formateaFecha(str(x), '%Y-%m-%d', '%d/%m/%Y'))
        df_opq["EMAIL_ADDRESS"] = df_opq["EMAIL_ADDRESS"].str.lower()

        return df_opq

    def get_user(self, email_address):
        sql = "select email, concat(lastname,', ',name) full_name, username, nombre, country_code, admission_date " \
              "      ,discharge_date, supervisor_complete_name, if(zone_pci=1, 'Si', '  ') zone_pci, 'OPECUS' origen" \
              "  from idm_view "
        if pd.isna(email_address) == False: # and len(email_address.strip()) > 0:
            sql += " WHERE lower(email) = '"+email_address.lower().strip()+"'"

        self.cur = self.conn_opq.cursor()
        self.cur.execute(sql)
        records = self.cur.fetchall()

        for i, row in enumerate(records):
            email = row[0].lower()
            end_date = "" if pd.isna(row[6]) else f.formateaFecha(str(row[6]), '%Y-%m-%d', '%d/%m/%Y')
            start_date = "" if pd.isna(row[5]) else f.formateaFecha(str(row[5]), '%Y-%m-%d', '%d/%m/%Y')
            return [email, row[1], row[2], row[3], row[4], start_date, end_date, row[7], row[8], row[9]]

        return[None, None, None, None, None, None, None, None, None, None]

    def close(self):
        self.conn_opq.close()


def getCSV():
    print("+ Connect CSV")
    cparser = ConfigParser()  # crea el objeto ConfigParser
    cparser.read("params/users.conf")
    c_csv_path = cparser.get("RRHH", "File Location")+"/RRHH.csv"
    print("- Connect CSV; path = ", c_csv_path)
    csv = pd.read_csv(c_csv_path)
    df_csv = pd.DataFrame(csv)

    # Elimino los legajos duplicados.
    df_csv.sort_values(['EMAIL_ADDRESS','TERMINATION_DATE'], ascending=[True, True], na_position='first', inplace=True)
    df_csv.drop_duplicates(subset="EMAIL_ADDRESS", keep='first', inplace=True)

    # Genero la columna "FULL_NAME" y dejo el mail en lowercase.
    df_csv["FULL_NAME"] = df_csv["APELLIDO"] + ", " + df_csv["PRIMER_NOMBRE"] + df_csv["SEGUNDO_NOMBRE"].apply(lambda x: " "+x if x is None else "")
    df_csv["EMAIL_ADDRESS"] = df_csv["EMAIL_ADDRESS"].str.lower()
    df_csv["ORIGEN"] = "RRHH"

    # Dejo sólo las columnas que me interesan, y las renombro.
    df_csv = df_csv[["FULL_NAME", "EMAIL_ADDRESS", "TERRITORY", "ENTERPRISE_HIRE_DATE", "MANAGER_NAME", "TERMINATION_DATE", "ORIGEN"]]
    df_csv.rename(columns={"TERRITORY": "COUNTRY_CODE", "ENTERPRISE_HIRE_DATE": "START_DATE", "MANAGER_NAME": "SUPERVISOR_NAME", "TERMINATION_DATE": "END_DATE"}, inplace=True)

    return df_csv


def getVPN():
    print("+ Busco VPN")
    cparser = ConfigParser()
    cparser.read("params/users.conf")
    # c_csv_path = cparser.get("RRHH", "File Location") + "/VPN.xlsx"
    # vpn = pd.read_excel(c_csv_path, skiprows=3)
    c_csv_path = cparser.get("RRHH", "File Location") + "/VPN.csv"
    vpn = pd.read_csv(c_csv_path, skiprows=3, sep=",")
    df_vpn = pd.DataFrame(vpn)

    # Hago adecuaciones de datos
    df_vpn["Email"] = df_vpn["Email"].str.lower()  # dejo el email en lowercase.
    # df_vpn["User Last Authentication Date"] = df_vpn["User Last Authentication Date"].apply(lambda x: f.formateaFecha(str(x), '%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M'))
    df_vpn["User Last Authentication Date"] = df_vpn["User Last Authentication Date"].apply(lambda x: f.formateaFecha(str(x), '%Y-%m-%d', '%d/%m/%Y %H:%M'))
    df_vpn["VPN_FULL_NAME"] = df_vpn["Last Name"] + ", " + df_vpn["First Name"]

    # Dejo sólo las columnas que me interesan y las renombro.
    df_vpn = df_vpn[["User ID", "VPN_FULL_NAME", "Email", "Account Enabled", "Token Enabled", "User Last Authentication Date"]]
    df_vpn.rename(columns={"User ID": "VPN_USER_NAME", "Email": "EMAIL_ADDRESS", "User Last Authentication Date": "VPN_LAST_LOGON_DATE",
                           "Account Enabled": "VPN_STATUS", "Token Enabled": "VPN_TOKEN_ENABLED"}, inplace=True)

    print(" "); print("df_vpn:"); print(df_vpn)
    f.quick_excel(df_vpn, "Rep_VPN")

    return df_vpn


def get_VF():
    # ----------------------------------------------------------------------------
    # CSV de VF! (de Tamara Patrignani)
    # ----------------------------------------------------------------------------
    print("+ Busco Viajes Falabella")
    vf_path = "C:/Users/daniel.vartabedian/Box Sync/DESPEGAR/Terminados/2019/20171127 - IDM - midPoint/Proyecto IDM/RRHH/Nómina VF.xlsx"
    vf = pd.read_excel(vf_path, sheet_name=None)  # leo la planilla con todos sus sheets (vf.keys())
    df_vf = pd.concat([vf["ARG"], vf["PER"], vf["COL"], vf["CHL"]])  # creo un df con los 4 sheets
    df_vf = df_vf[["EMAIL_ADDRESS", "FULL_NAME", "COUNTRY_CODE", "SUPERVISOR_NAME", "STATUS"]]  # dejo sólo las columnas que me interesan.
    df_vf["EMAIL_ADDRESS"] = df_vf["EMAIL_ADDRESS"].str.lower()  # dejo el email en lowercase.
    df_vf.rename(columns={"STATUS": "END_DATE"}, inplace=True)  # renombro campos para que coincida con df_csv

    return df_vf


def getSurgeMail():
    import re

    print("+ getSurgeMail")
    cparser = ConfigParser()
    cparser.read("params/users.conf")
    c_path = cparser.get("RRHH", "File Location") + "/nwauth.txt"
    print("- Busco SurgeMail; path = ", c_path)

    print("+ Parseo los datos del nwauth.txt")
    all_data = []
    with open(c_path, 'r') as f_in:
        for line in f_in:
            m = re.search(r'^(.*?):(.*?):', line)
            if not m:
                continue
            data = dict(re.findall(r'([^\s]+)="([^"]+)"', line.split(':', maxsplit=2)[-1]))
            data['mail'] = m.group(1)
            data['password'] = m.group(2)
            all_data.append(data)
    print("- Parseo los datos del nwauth.txt")

    df = pd.DataFrame(all_data) #.fillna('')
    print(" "); print("df (SurgeMail - original):"); print(df) #; print(df.info())
    f.quick_excel(df, "Rep_Surgemail (original)")

    # ----------------------------------------------------------------------------------------------------------------------------------
    # Elimino los registros que se encuentren cancelados (ojo porque es un literal libre, puede tener puntos u otras descripciones)
    # ----------------------------------------------------------------------------------------------------------------------------------
    df = df[(df['mailstatus'] != 'cancelled') & (df['mailstatus'] != 'closed') & (df['mailstatus'] != 'suspended')]

    print("+ Convierto las fechas (epoch)")
    # Convierto el formato epoch (segundos desde el 01/01/1970) a format dd/mm/yyyy hh:mi:ss.
    df["created"] = df["created"].apply(lambda x: datetime.datetime.fromtimestamp(int(x)).strftime('%d/%m/%Y %H:%M:%S') if pd.isna(x) == False else x)
    # df["last_login"] = df.apply(lambda row: row['created'] if pd.isna(row['last_login']) else
    #                             datetime.datetime.fromtimestamp(int(row['last_login'])).strftime('%d/%m/%Y %H:%M:%S'), axis=1)
    df["last_login"] = df.apply(lambda row: None if pd.isna(row['last_login']) else
                                datetime.datetime.fromtimestamp(int(row['last_login'])).strftime('%d/%m/%Y %H:%M:%S'), axis=1)
    print("- Convierto las fechas (epoch)")

    f.quick_excel(df, "Rep_Surgemail (completo)")

    # Dejo sólo los campos que me interesan y los renombro.
    df = df[["mail", "full_name", "mailstatus", "created", "last_login", "pais"]]
    df.rename(columns={'mail': "EMAIL_ADDRESS", 'full_name': "SM_FULL_NAME", 'mailstatus': "SM_STATUS",
                       'created': "SM_CREATION_DATE", 'last_login': "SM_LAST_LOGON_DATE", 'pais': "SM_COUNTRY"}, inplace=True)

    print(" "); print("df (SurgeMail - final):"); print(df) #; print(df.info())

    return df


# --------------------------------------------
# Recupera los usuarios de GSuite
# --------------------------------------------
def getGSuite(email_address=None):
    print("+ getGSuite")
    # If modifying these scopes, delete the file token.pickle.
    SCOPES = ['https://www.googleapis.com/auth/admin.directory.user']

    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first time.
    if os.path.exists('params/token.pickle'):
        with open('params/token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('params/credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('params/token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('admin', 'directory_v1', credentials=creds, cache_discovery=False)

    # Call the Admin SDK Directory API
    print('Getting the users in the domain')

    if email_address is None:
        # Recupero todos los usuarios paginando con el Token.
        pgToken = None
        while True:
            results = service.users().list(customer='my_customer', pageToken=pgToken, maxResults=500).execute()
            users = results.get('users', [])
            pgToken = results.get('nextPageToken')
            print('token = ', pgToken)
            try:
                df_tk = pd.DataFrame(users)
                df = pd.concat([df, df_tk])         # esto da un warning porque df no existe antes de la asignación, pero es un while y la 1a vuelta entra por la excepción.
            except:
                df = pd.DataFrame(users)
            if pgToken is None:
                break
    else:  # en caso de que se informe sólo 1 email, pero para que funcione el mail tiene que estar completo; i.e.: "pepe@despegar.com"
        results = service.users().get(userKey=email_address).execute()
        #df = pd.json_normalize(results)
        df = pd.DataFrame([results])

    # df = pd.DataFrame(users)
    print(" "); print("df original = "); print(df)
    f.quick_excel(df, "Rep_GSuite_original")

    # Filtro aquellos registros que estén en le OrgUnit de Suspendidos o estén con el flag de Suspendidos (no quiero estos registros):
    # df = df[(df['orgUnitPath'] != "/Suspended Users") | (df['suspended'] == False)]
    df = df[(df['orgUnitPath'] != "/Suspended Users")]
    df = df[(df['suspended'] == False)]

    # Hago transformaciones de datos.
    df['fullName'] = df['name'].apply(lambda x: x['fullName'])

    df['lastLoginTime'] = df['lastLoginTime'].apply(lambda x: datetime.datetime.strftime(datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%fZ'), "%d/%m/%Y %H:%M:%S"))
    df['creationTime'] = df['creationTime'].apply(lambda x: datetime.datetime.strftime(datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%fZ'), "%d/%m/%Y %H:%M:%S"))
    # df['lastLoginTime'] = df['lastLoginTime'].apply(lambda x: None if x < df["creationTime"] else x)
    # df.loc[df['lastLoginTime'] < df['creationTime'], 'lastLoginTime'] = None    # si la fecha del último login < fecha de creación, lo dejo en blanco.

    print(" "); print("df (con transformación de datos):"); print(df)

    # Dejo sólo las columnas que me interesan y además las renombro.
    df = df[['primaryEmail', 'fullName', 'lastLoginTime', 'creationTime', 'suspended', 'orgUnitPath']]
    df.rename(columns={'primaryEmail': "EMAIL_ADDRESS", 'fullName': "GS_FULL_NAME", 'lastLoginTime': "GS_LAST_LOGON_DATE",
                       'creationTime': "GS_CREATION_DATE", 'suspended': "SUSPENDED", 'orgUnitPath': "GS_ORG_UNIT"}, inplace=True)
    df["GS_ORIGEN"] = "GSUITE"
    print(" "); print("df GSuite (final): "); print(df)
    f.quick_excel(df, "Rep_GSuite_final")
    print("- getGSuite")

    return df



class getATP1():
    def __init__(self):
        l_file_location = "params/users.db"
        conndb = db.connect(l_file_location)
        df = pd.read_sql("Select * From Params Where Key = 'ATP1' Order by Id", conndb)

        # Creo las variables dinámicamente.
        for i in range(0, len(df.index)):
            globals()[str(df.loc[i, "Variable_Name"]).lower()] = df.loc[i, "Value"]

        print("  host = ", l_host)
        print("  port = ", l_port)
        print("  database = ", l_database)
        print("  user = ", l_user)
        print("- Recupera parámetros")

        print("+ Conexión a ATP1")
        try:
            self.conn = mysql.connector.connect(host=l_host,
                                                port=l_port,
                                                database=l_database,
                                                user=l_user,
                                                password=f.dec(l_pwd))
            print("Conexión ATP1 exitosa")
        except Exception as err:
            print("Error en la conexión a ATP1; ", err)
            return
        print("- Conexión a ATP1")


    def get_actives(self):
        sql = "select us.us_email, concat(us.us_lastname,', ',us.us_firstname) full_name, cr.cr_username, rc.name country " \
              "      ,if(cr.cr_enabled=1,'Activo','Inactivo') enabled , cr.cr_lastdatelogin, 'ATP1' origen " \
              "  from RD_COUNTRY  rc " \
              "      ,CREDENTIALS cr " \
              "      ,USERS       us " \
              " where cr.us_oid = us.oid " \
              "   and rc.OID = us.us_country " \
              "   and cr.cr_enabled = 1; "   # antes puse <= 1
        print(sql)
        self.cur = self.conn.cursor()
        print("+ Ejecuta sql")
        self.cur.execute(sql)
        print("- Ejecuta sql")
        recs = self.cur.fetchall()

        # Genero el df
        df_atp1 = pd.DataFrame(recs, columns=["EMAIL_ADDRESS", "ATP1_FULL_NAME", "ATP1_USERNAME", "ATP1_COUNTRY", "ATP1_STATUS", "ATP1_LAST_LOGON_DATE", "ATP1_ORIGEN"])
        # Cierro la conexión.
        self.conn.close()

        print(" "); print("df_atp1:"); print(df_atp1); print(df_atp1.info())
        f.quick_excel(df_atp1, "Rep_ATP1 (original)")

        # Convierto datos.
        df_atp1["ATP1_LAST_LOGON_DATE"] = df_atp1["ATP1_LAST_LOGON_DATE"].apply(lambda x: f.formateaFecha(str(x), '%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S'))
        df_atp1["EMAIL_ADDRESS"] = df_atp1["EMAIL_ADDRESS"].str.lower()
        # df_atp1["ATP1_USERNAME"] = df_atp1["ATP1_USERNAME"].apply(lambda x: x.decode("utf-8"))

        return df_atp1



class getATP1_PCI():
    def __init__(self):
        l_file_location = "params/users.db"
        conndb = db.connect(l_file_location)
        df = pd.read_sql("Select * From Params Where Key = 'ATP1_PCI' Order by Id", conndb)

        print(" "); print("df (select params):"); print(df)

        # Creo las variables dinámicamente.
        for i in range(0, len(df.index)):
            globals()[str(df.loc[i, "Variable_Name"]).lower()] = df.loc[i, "Value"]

        print("  host = ", l_host)
        print("  port = ", l_port)
        print("  database = ", l_database)
        print("  user = ", l_user)
        print("- Recupera parámetros")

        print("+ Conexión a ATP1_PCI")
        try:
            self.conn = mysql.connector.connect(host=l_host,
                                                port=l_port,
                                                database=l_database,
                                                user=l_user,
                                                password=f.dec(l_pwd))
            print("Conexión ATP1_PCI exitosa")
        except Exception as err:
            print("Error en la conexión a ATP1_PCI; ", err)
            return
        print("- Conexión a ATP1_PCI")


    def get_actives(self):
        sql = "select us.us_email, concat(us.us_lastname,', ',us.us_firstname) full_name, cr.cr_username, rc.name country " \
              "      ,if(cr.cr_enabled=1,'Activo','Inactivo') enabled , cr.cr_lastdatelogin, 'ATP1_PCI' origen " \
              "  from RD_COUNTRY  rc " \
              "      ,CREDENTIALS cr " \
              "      ,USERS       us " \
              " where cr.us_oid = us.oid " \
              "   and rc.OID = us.us_country " \
              "   and cr.cr_enabled = 1; "         # antes decia <= 1
        print(sql)
        self.cur = self.conn.cursor()
        print("+ Ejecuta sql")
        self.cur.execute(sql)
        print("- Ejecuta sql")
        recs = self.cur.fetchall()

        # Genero el df
        df_atp1 = pd.DataFrame(recs, columns=["EMAIL_ADDRESS", "ATP1_FULL_NAME", "ATP1_USERNAME", "ATP1_COUNTRY", "ATP1_STATUS", "ATP1_LAST_LOGON_DATE", "ATP1_ORIGEN"])
        # Cierro la conexión.
        self.conn.close()

        # Convierto datos.
        df_atp1["ATP1_LAST_LOGON_DATE"] = df_atp1["ATP1_LAST_LOGON_DATE"].apply(lambda x: f.formateaFecha(str(x), '%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S'))
        df_atp1["EMAIL_ADDRESS"] = df_atp1["EMAIL_ADDRESS"].str.lower()

        return df_atp1



class getATP3():
    def __init__(self):
        l_file_location = "params/users.db"
        conn = db.connect(l_file_location)
        df = pd.read_sql("Select * From Params Where Key = 'ATP3' Order by Id", conn)

        print(" "); print("df (select params):"); print(df)

        # Creo las variables dinámicamente.
        for i in range(0, len(df.index)):
            globals()[str(df.loc[i, "Variable_Name"]).lower()] = df.loc[i, "Value"]

        print("  host = ", l_host)
        print("  port = ", l_port)
        print("  database = ", l_database)
        print("  user = ", l_user)
        print("- Recupera parámetros")

        print("+ Conexión a ATP3")
        try:
            self.conn = mysql.connector.connect(host=l_host,
                                                port=l_port,
                                                database=l_database,
                                                user=l_user,
                                                password=f.dec(l_pwd))
            print("Conexión ATP3 exitosa")
        except Exception as err:
            print("Error en la conexión a ATP3; ", err)
            return
        print("- Conexión a ATP3")


    def get_actives(self):
        sql = "SELECT u.email, CONCAT(u.lastname, ', ', u.name) as fullname, u.username, u.source, u.created start_date " \
              "      ,(select max(le.`date`) from atp.login_events le where le.user_id = u.id) as last_login " \
              "      ,(case when u.deleted = true or u.active = false then 'Inactivo' else 'Activo' end) as activo, 'ATP3' origen " \
              "  FROM atp.user as u " \
              " WHERE (case when u.deleted = true or u.active = false then 'NO' else 'SI' end) = 'SI' "

        print(sql)
        self.cur = self.conn.cursor()
        print("+ Ejecuta sql")
        self.cur.execute(sql)
        print("- Ejecuta sql")
        recs = self.cur.fetchall()

        # Genero el df
        df_atp3 = pd.DataFrame(recs, columns=["EMAIL_ADDRESS", "ATP3_FULL_NAME", "ATP3_USERNAME", "ATP3_SOURCE",
                                              "ATP3_START_DATE", "ATP3_LAST_LOGON_DATE", "ATP3_STATUS", "ATP3_ORIGEN"])
        # Cierro la conexión.
        self.conn.close()

        # Convierto datos.
        df_atp3["ATP3_START_DATE"] = df_atp3["ATP3_START_DATE"].apply(lambda x: f.formateaFecha(str(x), '%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S'))
        df_atp3["ATP3_LAST_LOGON_DATE"] = df_atp3["ATP3_LAST_LOGON_DATE"].apply(lambda x: f.formateaFecha(str(x), '%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S'))
        df_atp3["EMAIL_ADDRESS"] = df_atp3["EMAIL_ADDRESS"].str.lower()
        print("df_atp3_pci.shape = ", df_atp3.shape)

        return df_atp3



class getATP3_PCI():
    def __init__(self):
        l_file_location = "params/users.db"
        conn = db.connect(l_file_location)
        df = pd.read_sql("Select * From Params Where Key = 'ATP3_PCI' Order by Id", conn)

        print(" "); print("df (select params):"); print(df)

        # Creo las variables dinámicamente.
        for i in range(0, len(df.index)):
            globals()[str(df.loc[i, "Variable_Name"]).lower()] = df.loc[i, "Value"]

        print("  host = ", l_host)
        print("  port = ", l_port)
        print("  database = ", l_database)
        print("  user = ", l_user)
        print("- Recupera parámetros")

        print("+ Conexión a ATP3-PCI")
        try:
            self.conn = mysql.connector.connect(host=l_host,
                                                port=l_port,
                                                database=l_database,
                                                user=l_user,
                                                password=f.dec(l_pwd))
            print("Conexión ATP3-PCI exitosa")
        except Exception as err:
            print("Error en la conexión a ATP3-PCI; ", err)
            return
        print("- Conexión a ATP3-PCI")


    def get_actives(self):
        sql = "SELECT u.email, CONCAT(u.lastname, ', ', u.name) as fullname, u.username, u.source, u.created start_date " \
              "      ,(select max(le.`date`) from atp.login_events le where le.user_id = u.id) as last_login " \
              "      ,(case when u.deleted = true or u.active = false then 'Inactivo' else 'Activo' end) as activo, 'ATP3-PCI' origen " \
              "  FROM atp.user as u " \
              " WHERE (case when u.deleted = true or u.active = false then 'NO' else 'SI' end) = 'SI' "

        print(sql)
        self.cur = self.conn.cursor()
        print("+ Ejecuta sql")
        self.cur.execute(sql)
        print("- Ejecuta sql")
        recs = self.cur.fetchall()

        # Genero el df
        df_atp3 = pd.DataFrame(recs, columns=["EMAIL_ADDRESS", "ATP3_FULL_NAME", "ATP3_USERNAME", "ATP3_SOURCE",
                                              "ATP3_START_DATE", "ATP3_LAST_LOGON_DATE", "ATP3_STATUS", "ATP3_ORIGEN"])
        # Cierro la conexión.
        self.conn.close()

        # Convierto datos.
        df_atp3["ATP3_START_DATE"] = df_atp3["ATP3_START_DATE"].apply(lambda x: f.formateaFecha(str(x), '%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S'))
        df_atp3["ATP3_LAST_LOGON_DATE"] = df_atp3["ATP3_LAST_LOGON_DATE"].apply(lambda x: f.formateaFecha(str(x), '%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S'))
        df_atp3["EMAIL_ADDRESS"] = df_atp3["EMAIL_ADDRESS"].str.lower()

        return df_atp3



# --------------------------------------------
# Conecta con users.db
# --------------------------------------------
class getUsersDB():
    def __init__(self):
        cp = f.readConf()
        l_file_location = cp.get("DB", "File Location")
        print("  DB Localization: ", l_file_location)
        self.conn = db.connect(l_file_location)

    def getData(self, sql):
        print("+ getData")
        # print(sql)
        df = pd.read_sql(sql, self.conn)
        # print(df)
        print("- getData")
        return df

    def insert(self, report_id, complete_report_flag, execute_each, time_unit, execute_days, start_date, end_date):
        print("+ Insert")
        l_complete_report_flag = 'Y' if complete_report_flag else "N"
        l_execute_days = "" if execute_days is None else execute_days

        print(report_id)
        print(l_complete_report_flag)
        print(execute_each)
        print(time_unit)
        print(l_execute_days)
        print(start_date)
        print(end_date)

        sql = "INSERT INTO Scheduler (Report_Id, Complete_Report_Flag, Enabled_Flag, Execute_Each, " \
              "                       Time_Unit, Execute_Days, Start_Date, End_Date, Next_Execution) " \
              "VALUES ('"+str(report_id)+"', '"+l_complete_report_flag+"', 'Y', '"+str(execute_each)+"', '"+time_unit+"', "+ \
                      l_execute_days+", '"+start_date+"', '"+end_date+"', '"+start_date+"');"
        print(sql)

        try:
            self.conn.execute(sql)
        except Exception as err:
            print("ERROR al hacer el Insert; ", err)
        print("- Insert")

    def update_next_exec(self, schedule_id, next_exec_str):
        cur = self.conn.cursor()
        sql = "UPDATE Scheduler " \
              "   SET Status = 'P'" \
              "      ,Next_Execution = '" + next_exec_str + "'" \
              " WHERE Schedule_Id = " + str(schedule_id) + " " \
              "   AND Enabled_Flag = 'Y';"
        print(sql)
        cur.execute(sql)
        self.conn.commit()

    def close(self):
        self.conn.close()


class getAD():
    def __init__(self):
        print("+ getAD")

        # Si el archivo existe lo lee y carga las variables en pantalla.
        cparser = ConfigParser()  # crea el objeto ConfigParser
        cparser.read("params/users.conf")

        # Constantes para AD.
        c_ad_server_name = cparser.get("AD", "Server") + ":" + cparser.get("AD", "Port")
        c_ad_user_name = cparser.get("AD", "User Name")
        c_ad_password = f.dec(cparser.get("AD", "Password"))
        self.c_ad_search_base = cparser.get("AD", "Search Base")
        self.c_ad_attributes = ['mail', 'userPrincipalName', 'userAccountControl', 'lastLogonTimestamp', 'cn', 'distinguishedName', 'sAMAccountName']

        # Conexion y busqueda.
        try:
            # print("Server:", c_ad_server_name)
            server = Server(c_ad_server_name, get_info=ALL)
            # print("Connection")
            self.conn = Connection(server, user=c_ad_user_name, password=c_ad_password, auto_bind=True)
            # print("Conexión al AD exitosa")
            print("- getAD")
        except Exception as err:
            print("Error en la conexión con el AD; ", err)
            return


    def get_samAccountName(self, p_email_address):
        if p_email_address is None:
            # print("El mail está vacío")
            return [None, None, None, None]

        l_email = p_email_address.lower().strip()
        try:
            if len(l_email) > 0:
                self.conn.search(search_base=self.c_ad_search_base, search_filter='(&(mail='+l_email+'))',
                                 attributes=self.c_ad_attributes, search_scope=SUBTREE)

            for row in self.conn.entries:
                self.l_userAccountControl = f.getADStatusCode(row.userAccountControl)
                self.l_lastlogontimestamp = f.formateaFecha(str(row.lastlogontimestamp), '%Y-%m-%d %H:%M:%S.%f%z', '%d/%m/%Y %H:%M')
                self.l_sAMAccountName = row.sAMAccountName

                if self.l_userAccountControl.lower().find("disabled") > 0:
                    return [self.l_sAMAccountName, self.l_lastlogontimestamp, self.l_userAccountControl, len(self.conn.entries)]

            return [self.l_sAMAccountName, self.l_lastlogontimestamp, self.l_userAccountControl, len(self.conn.entries)]

        except Exception as err:
            # print("Error! email_address =", l_email, '; Error: ', err)
            return[None, None, None, None]


    def get_actives(self):
        # -----------------------------------
        # ACTIVOS
        # -----------------------------------
        print("+ AD Activos")
        c_attributes = ['mail', 'userPrincipalName', 'userAccountControl', 'lastLogonTimestamp', 'cn', 'distinguishedName', 'sAMAccountName', 'whenCreated']
        c_filter_act = '(&(objectCategory=person)(objectClass=user)(!(userAccountControl:1.2.840.113556.1.4.803:=2)))'             # Enabled Users

        # Genero el df y cambio el nombre de los campos.
        df_act = self.get_accounts(c_attributes, c_filter_act)

        # Renombro algunas columnas para hacerlas coincidir con las de df (para que funcione el combine_first).
        df_act.rename(columns={"mail": "EMAIL_ADDRESS", "userAccountControl": "AD_STATUS", "lastLogonTimestamp": "AD_LAST_LOGON_DATE",
                               "cn": "AD_FULL_NAME", "distinguishedName": "AD_COUNTRY_CODE", "sAMAccountName": "AD_SAMACCOUNTNAME",
                               "whenCreated": "AD_CREATION_DATE"}, inplace=True)

        print(" "); print("df_ad_activos (df_act) (original):"); print(df_act)

        """
        # ------------------------------------------------------------
        # Agrego VPN y GSuite para traer el Last_Logon más reciente
        # ------------------------------------------------------------
        df_vpn = getVPN()
        df_vpn = df_vpn[["EMAIL_ADDRESS", "VPN_LAST_LOGON_DATE"]]

        df_gs = getGSuite()
        df_gs = df_gs[["EMAIL_ADDRESS", "GS_LAST_LOGON_DATE"]]

        # df_sm = getSurgeMail()

        df_act = pd.merge(left=df_act, right=df_vpn, how='left', left_on='EMAIL_ADDRESS', right_on='EMAIL_ADDRESS')
        df_act = pd.merge(left=df_act, right=df_gs, how='left', left_on='EMAIL_ADDRESS', right_on='EMAIL_ADDRESS')
        # df_act = pd.merge(left=df_act, right=df_sm, how='left', left_on='EMAIL_ADDRESS', right_on='EMAIL_ADDRESS')

        # Convierto las columnas de fecha a formato date (sinó el cálculo del max, da error porque no saca max de strings).
        df_act["AD_LAST_LOGON_DATE"] = pd.to_datetime(df_act["AD_LAST_LOGON_DATE"])
        df_act["VPN_LAST_LOGON_DATE"] = pd.to_datetime(df_act["VPN_LAST_LOGON_DATE"])
        df_act["GS_LAST_LOGON_DATE"] = pd.to_datetime(df_act["GS_LAST_LOGON_DATE"])

        # print(" "); print("df_nomina (formateadas las fechas como date):"); print(df_nomina)

        # Calculo el last_logon como el last_logon de todas las app
        print("+ Calcula el Max(Last_Logon_Date)")
        df_act["MAX_LAST_LOGON_DATE"] = df_act[["AD_LAST_LOGON_DATE", "VPN_LAST_LOGON_DATE", "GS_LAST_LOGON_DATE"]].max(axis=1, skipna=True)
        # df_nomina["MAX_LAST_LOGON"] = np.nanmax(df_nomina["AD_LAST_LOGON_DATE", "VPN_LAST_LOGON_DATE", "GS_LAST_LOGON_DATE"].values, axis=0) # este método es más rápido que el anterior.

        print(" "); print("df_act (con Max_Last_Logon):"); print(df_act)

        df_act["MAX_LAST_LOGON_DATE"] = df_act["MAX_LAST_LOGON_DATE"].apply(
            lambda x: f.formateaFecha(str(x), '%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M'))
        df_act["AD_LAST_LOGON_DATE"] = df_act["AD_LAST_LOGON_DATE"].apply(
            lambda x: f.formateaFecha(str(x), '%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M'))
        df_act["VPN_LAST_LOGON_DATE"] = df_act["VPN_LAST_LOGON_DATE"].apply(
            lambda x: f.formateaFecha(str(x), '%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M'))

        # print(" "); print("df_nomina (con Max_Last_Logon formateado como string):"); print(df_nomina)
        print("- Calcula el Max(Last_Logon_Date)")



        # ----------------------------------------------
        # Calculo el AD_ELAPSED_DAYS
        # ----------------------------------------------
        for i in range(0, len(df_act.index)):
            if pd.isna(df_act.loc[i, "MAX_LAST_LOGON_DATE"]): # or len(str(df.loc[i, "AD_LAST_LOGON_DATE"])) < 7:
                if pd.isna(df_act.loc[i, "AD_CREATION_DATE"]): # or len(str(df.loc[i, "START_DATE"])) < 7:
                    df_act.loc[i, "AD_ELAPSED_DAYS"] = 0
                else:
                    df_act.loc[i, "AD_ELAPSED_DAYS"] = abs(datetime.datetime.now() - datetime.datetime.strptime(df_act.loc[i, "AD_CREATION_DATE"], "%d/%m/%Y")).days
            else:
                df_act.loc[i, "AD_ELAPSED_DAYS"] = abs(datetime.datetime.now() - datetime.datetime.strptime(df_act.loc[i, "MAX_LAST_LOGON_DATE"], "%d/%m/%Y %H:%M")).days

        f.quick_excel(df_act,"Rep_AD_Max_Logon")

        # Borro las columnas de fecha que agregué para hacer el cálculo (porque después quedan como _x e _y en los merge de cada apps).
        df_act.drop(["VPN_LAST_LOGON_DATE", "GS_LAST_LOGON_DATE"], axis='columns', inplace=True)
        """

        print("- AD Activos")
        return df_act


    def get_inactives(self):
        # -----------------------------------
        # INACTIVOS
        # -----------------------------------
        print("+ AD Inactivos")
        c_attributes = ['mail', 'userPrincipalName', 'userAccountControl', 'lastLogonTimestamp', 'cn', 'distinguishedName', 'sAMAccountName', 'whenCreated']
        c_filter_off = '(&(objectCategory=person)(objectClass=user)(userAccountControl:1.2.840.113556.1.4.803:=2))'                # Disabled Users

        df_inact = self.get_accounts(c_attributes, c_filter_off)

        # Renombro algunas columnas para hacerlas coincidir con las de df (para que funcione el combine_first).
        df_inact.rename(columns={"mail": "EMAIL_ADDRESS", "userAccountControl": "AD_STATUS", "lastLogonTimestamp": "AD_LAST_LOGON_DATE",
                                 "cn": "AD_FULL_NAME", "distinguishedName": "AD_COUNTRY_CODE", "sAMAccountName": "AD_SAMACCOUNTNAME",
                                 "whenCreated": "AD_CREATION_DATE"}, inplace=True)
        print("- AD Inactivos")
        return df_inact


    def get_accounts(self, attr, filter):
        print("+ Get_Accounts")
        # Hago la búsqueda en el AD.
        self.conn.search(search_base=self.c_ad_search_base, search_filter=filter, attributes=attr, search_scope=SUBTREE)

        # Creo un df vacío.
        df_ad = pd.DataFrame(columns=attr)

        for row in self.conn.entries:
            l_dn = ''.join(row.distinguishedName)
            if not ('OU=Service Users' in l_dn.split(sep=",")):
                l_mail = (''.join(row.mail)).lower()
                l_status = f.getADStatusCode(row.userAccountControl)
                l_lastlogon = f.formateaFecha(str(row.lastlogontimestamp), '%Y-%m-%d %H:%M:%S.%f%z', '%d/%m/%Y %H:%M')
                l_samaccount = ''.join(row.sAMAccountName)
                l_cn = ''.join(row.cn)
                l_principalName = (''.join(row.userPrincipalName))
                l_creation_date = f.formateaFecha(str(row.whenCreated)[0:-6], '%Y-%m-%d %H:%M:%S', '%d/%m/%Y')

                # Busco la OU dentro del distinguishedName, para obtener el Country.
                try:
                    start = l_dn.find("DC=") + 3
                    end = l_dn.find(",", start)
                    l_country = l_dn[start:end].upper()
                except:
                    l_country = ""

                if l_mail is not None and len(l_mail) > 0:
                    lin = [l_mail, l_principalName, l_status, l_lastlogon, l_cn, l_country, l_samaccount, l_creation_date]
                    df_ad = df_ad.append(pd.Series(lin, index=attr), ignore_index=True)
        print("- Get_Accounts")

        return df_ad


    def get_computers(self):
        # -----------------------------------
        # ACTIVOS
        # -----------------------------------
        print("+ AD Computers")
        c_attributes = ['sAMAccountName', 'cn', 'userAccountControl', 'lastLogonTimestamp', 'distinguishedName', 'whenCreated', 'operatingSystem', 'operatingSystemVersion']
        c_filter_act = '(&(objectCategory=computer))'

        # Genero el df y cambio el nombre de los campos.
        # df_act = self.get_accounts(c_attributes, c_filter_act)
        self.conn.search(search_base=self.c_ad_search_base, search_filter=c_filter_act, attributes=c_attributes, search_scope=SUBTREE)

        # Creo un df vacío.
        df_ad = pd.DataFrame(columns=c_attributes)

        for row in self.conn.entries:
            l_dn = ''.join(row.distinguishedName)
            if not ('OU=Service Users' in l_dn.split(sep=",")):
                l_samaccount = ''.join(row.sAMAccountName)
                l_cn = ''.join(row.cn)
                l_status = f.getADStatusCode(row.userAccountControl)
                l_lastlogon = f.formateaFecha(str(row.lastlogontimestamp), '%Y-%m-%d %H:%M:%S.%f%z', '%d/%m/%Y %H:%M')
                l_creation_date = f.formateaFecha(str(row.whenCreated)[0:-6], '%Y-%m-%d %H:%M:%S', '%d/%m/%Y')
                l_so = (''.join(row.operatingSystem))
                l_so_version = (''.join(row.operatingSystemVersion))

                # Busco la OU dentro del distinguishedName, para obtener el Country.
                try:
                    start = l_dn.find("DC=") + 3
                    end = l_dn.find(",", start)
                    l_country = l_dn[start:end].upper()
                except:
                    l_country = ""

                lin = [l_samaccount, l_cn, l_status, l_lastlogon, l_dn, l_creation_date, l_so, l_so_version, l_country]
                attr = c_attributes + ["AD_COUNTRY"]
                df_ad = df_ad.append(pd.Series(lin, index=attr), ignore_index=True)

        # Filtro los servidores, dejo sólo las computadoras (y notebooks) que están en la OU=Computadoras.
        # for i, row in enumerate(df_ad):
        #     s = re.search("Computadoras", row, re.IGNORECASE)
        #     if pd.isna(s):
        #         continue
        #     else:
        #         if s.start() >= 0:
        # df_ad = df_ad["distinguishedName"].apply(lambda x: True if not pd.isna(re.search("Computadoras", x, re.IGNORECASE)) and re.search("Computadoras", x, re.IGNORECASE).start() >= 0 else False)

        print(" "); print("df_ad (original)"); print(df_ad)

        df2 = df_ad[df_ad["distinguishedName"].apply(lambda x: not pd.isna(x))]

        print(" "); print("df2 (sin valores nulos)"); print(df2)

        df_ad = df2[df2["distinguishedName"].apply(lambda x: not pd.isna(re.search("Computadoras", x, re.IGNORECASE)))]

        print(" "); print("df_ad (final)"); print(df_ad)

        # Renombro algunas columnas para hacerlas coincidir con las de df (para que funcione el combine_first).
        df_ad.rename(columns={"sAMAccountName": "AD_SAMACCOUNTNAME", "cn": "AD_FULL_NAME", "userAccountControl": "AD_STATUS",
                              "lastLogonTimestamp": "AD_LAST_LOGON_DATE", "distinguishedName": "AD_COUNTRY_CODE", "whenCreated": "AD_CREATION_DATE",
                              "operatingSystem": "AD_SO", "operatingSystemVersion": "AD_SO_VERSION"}, inplace=True)

        # ----------------------------------------------
        # Calculo el AD_ELAPSED_DAYS
        # ----------------------------------------------
        for i in range(0, len(df_ad.index)):
            if pd.isna(df_ad.loc[i, "AD_LAST_LOGON_DATE"]):
                if pd.isna(df_ad.loc[i, "AD_CREATION_DATE"]):
                    df_ad.loc[i, "AD_ELAPSED_DAYS"] = 0
                else:
                    df_ad.loc[i, "AD_ELAPSED_DAYS"] = abs(datetime.datetime.now() - datetime.datetime.strptime(df_ad.loc[i, "AD_CREATION_DATE"], "%d/%m/%Y")).days
            else:
                df_ad.loc[i, "AD_ELAPSED_DAYS"] = abs(datetime.datetime.now() - datetime.datetime.strptime(df_ad.loc[i, "AD_LAST_LOGON_DATE"], "%d/%m/%Y %H:%M")).days

        print("- AD Computers")
        return df_ad


    def close_conn(self):
        # Cierro la conexión.
        self.conn.unbind()


def nomina():
    # ----------------------------------------------
    # RRHH
    # ----------------------------------------------
    df_csv = getCSV()

    print(" "); print("df_csv (rrhh):"); print(df_csv)
    f.quick_excel(df_csv, "Rep_Nomina1 - CSV")
    # ----------------------------------------------
    # Opecus
    # ----------------------------------------------
    opq = getOpecus()
    df_opq = opq.get_actives()
    df_opq_inact = opq.get_inactives()
    opq.close()
    df_opq = pd.concat([df_opq, df_opq_inact])

    # Si hay duplicados, dejo sólo los registros Activos.
    df_opq.sort_values(['EMAIL_ADDRESS', 'OPQ_STATUS'], ascending=[True, True], na_position='last', inplace=True)
    df_opq.drop_duplicates(subset="EMAIL_ADDRESS", keep='first', inplace=True)

    print(" "); print("df_opq:"); print(df_opq)
    f.quick_excel(df_opq, "Rep_Nomina2 - Opecus")

    # ==============================================
    # Genero el df_nomina: concateno RRHH + Opecus
    # ==============================================
    df_nomina = pd.concat([df_csv, df_opq])

    print(" "); print("df_nomina (RRHH + Opecus):"); print(df_nomina)
    f.quick_excel(df_nomina, "Rep_Nomina3 - Nomina=CSV+Opq")

    # --------------------------------------------------------------------------------------------------
    # Traigo el AD (dejo 1 solo registro activo, porque un mismo usuario puede tener varios registros).
    # --------------------------------------------------------------------------------------------------
    ad = getAD()
    df_ad = ad.get_actives()
    df_ad_inact = ad.get_inactives()
    # df_ad = df_ad.append(df_ad_inact)
    df_ad = pd.concat([df_ad, df_ad_inact])

    # OJO! con esta condición, porque si encuentra n registros en el AD, trae el del login más cercano (es correcto esto?)
    df_ad.sort_values(['EMAIL_ADDRESS', 'AD_LAST_LOGON_DATE'], ascending=[True, True], na_position='last', inplace=True)
    df_ad.drop_duplicates(subset="EMAIL_ADDRESS", keep='first', inplace=True)

    print(" "); print("df_ad:"); print(df_ad)
    f.quick_excel(df_ad, "Rep_Nomina4 - AD")

    # ========================================================================================================
    # Genero el df con nómina + AD (con inner join, porque me interesa que matchee sólo las coincidencias).
    # Aqui detecté un problema no resuelto aún: si un usuario está de baja en AD pero de alta en RRHH u Opecus,
    # no se informa; esto es un error porque si está activo en RRRHH u Opecus, entonces debe informarse. DV-12/01/2021.
    # ========================================================================================================
    df = pd.merge(left=df_nomina, right=df_ad, how='inner', left_on='EMAIL_ADDRESS', right_on='EMAIL_ADDRESS')

    # Al sumar RRHH + Opecus, pueden generarse duplicados; dejo sólo los registros activos.
    df.sort_values(['EMAIL_ADDRESS', 'END_DATE'], ascending=[True, True], na_position='first', inplace=True)
    df.drop_duplicates(subset="EMAIL_ADDRESS", keep='first', inplace=True)

    print(" "); print("df (nómina + AD):"); print(df)
    f.quick_excel(df, "Rep_Nomina5End - CSV+Opq+AD)")

    # ------------------------------------------------------------------------------------------------
    # Marco los duplicados en la columna Comentarios (aquí es donde se crea la columna COMMENTS).
    # ------------------------------------------------------------------------------------------------
    # df["COMMENTS"] = df.duplicated(["EMAIL_ADDRESS"], keep=False)     # marca con True los registtos duplicados.
    # df["COMMENTS"] = df["COMMENTS"].apply(lambda x: "Revisar origen (registro duplicado)" if x else None)

    # print(" "); print("df_nomina (final):"); print(df)
    # f.quick_excel(df, "Rep_Nomina (final)")

    return df


def get_SOX():
    # ----------------------------------------------------------------------------
    # Lee la planilla "Reporte aplicaciones SOX.xlsx" (en RRHH)
    # ----------------------------------------------------------------------------
    cparser = ConfigParser()  # crea el objeto ConfigParser
    cparser.read("params/users.conf")
    c_path = cparser.get("RRHH", "File Location")+"/Reportes aplicaciones SOX.xlsx"
    print("- Connect CSV; path = ", c_path)
    # sox = pd.read_excel(c_path, sheet_name=None)  # leo la planilla con todos sus sheets (vf.keys())
    # print("Tabs de la planilla leída: "); print(sox.keys())
    # df_sox = sox.concat(sox)
    # f.quick_excel(df_sox, "Rep_SOX_original")

    # ----------------------------------------------
    # Agrego una columna con el nombre del sheet.
    # ----------------------------------------------
    workbook = pd.ExcelFile(c_path)
    sheets = workbook.sheet_names

    # Esto agrega una columna llamada sheet_name con el nombre de cada sheet.
    df_sox = pd.concat([pd.read_excel(workbook, sheet_name=s).assign(sheet_name=s) for s in sheets])

    # ----------------------------------------------
    # Renombro los campos y normalizo formatos.
    # ----------------------------------------------
    df_sox.rename(columns={"Correo": "EMAIL_ADDRESS", "Usuario": "SOX_USER_NAME", "Nombre": "SOX_FULL_NAME",
                           "Last login": "SOX_LAST_LOGON_DATE", "Fecha de creacion": "SOX_START_DATE", "sheet_name": "SOX_APPS"}, inplace=True)

    df_sox["EMAIL_ADDRESS"] = df_sox["EMAIL_ADDRESS"].str.lower()

    return df_sox

