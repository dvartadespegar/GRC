from ldap3 import Server, Connection, ALL, SUBTREE
from ldap3.core.exceptions import LDAPCursorError
from PyQt5.QtWidgets import QTableWidgetItem
from PyQt5 import QtWidgets
import pandas as pd
import threading
from configparser import ConfigParser

import funcs as f
import connections as co


def buscar(p_eMail, p_FullName, p_tblAD, p_tblOracle, p_tblCSV, p_tblOpecus, p_tblVPN, p_tblSurge, p_tblGMail):
    # ========================================================================
    # THREAD para el procesamiento del df (para independizar el progress_bar)
    # ========================================================================
    print("+ thread_get_users")
    hilo = thread_get_users(args=(p_eMail, p_FullName, p_tblAD, p_tblOracle, p_tblCSV, p_tblOpecus, p_tblVPN, p_tblSurge, p_tblGMail, ), daemon=False)
    print("- thread_get_users")
    hilo.start()

    print("isAlive? (después del while) = ", hilo.is_alive())
    print("/======= FIN GET_USERS.BUSCAR =======/")


class thread_get_users(threading.Thread):
    def __init__(self, group=None, target=None, name=None, args=(), kwargs=None, *, daemon=None):
        super().__init__(group=group, target=target, name=name, daemon=daemon)
        print(args)
        self.txteMail = args[0]     # p_txteMail
        self.txtName = args[1]      # p_txtName
        self.tblAD = args[2]        # p_tblAD
        self.tblOracle = args[3]    # p_tblOracle
        self.tblCSV = args[4]       # p_tblCSV
        self.tblOpecus = args[5]    # p_tblOpecus
        self.tblVPN = args[6]       # p_tblVPN
        self.tblSurge = args[7]     # p_tblSurge
        self.tblGMail = args[8]     # p_tblGMail

        print("+ Winbar")
        self.prog = f.winbar()
        self.prog.show()
        print("- Winbar")


    def run(self):
        print("+ Llamo a Oracle")
        self.prog.adv(5)
        self.tblOracle.setRowCount(0)
        self.getOracle()
        self.prog.adv(15)
        print("- Llamo a Oracle")

        print("+ Llamo a Opecus")
        self.tblOpecus.setRowCount(0)
        self.getOpecus()
        self.prog.adv(30)
        print("+ Llamo a Opecus")

        print("+ Llamo a AD")
        self.tblAD.setRowCount(0)
        self.getAD()
        self.prog.adv(55)
        print("- Llamo a AD")

        print("+ Llamo CSV")
        self.tblCSV.setRowCount(0)
        # df_csv = co.getCSV()
        # self.getCSV(df_csv)
        self.getCSV()
        print("- Llamo CSV")
        self.prog.adv(70)

        print("+ Llamo VPN")
        self.tblVPN.setRowCount(0)
        df_vpn = co.getVPN()
        self.getVPN(df_vpn)
        print("- Llamo VPN")
        self.prog.adv(80)

        print("+ Llamo SurgeMail")
        self.tblSurge.setRowCount(0)
        self.getSurge()
        print("- Llamo SurgeMail")
        self.prog.adv(85)

        print("+ Llamo GMail")
        self.tblGMail.setRowCount(0)
        self.getGMail()
        print("- Llamo GMail")
        self.prog.adv(95)

    def getAD(self):
        print("/======= INICIO AD =======/")

        # Si el archivo existe lo lee y carga las variables en pantalla.
        cparser = ConfigParser()  # crea el objeto ConfigParser
        cparser.read("params/users.conf")

        # Constantes para AD.
        c_ad_server_name = cparser.get("AD", "Server") + ":" + cparser.get("AD", "Port")
        c_ad_user_name = cparser.get("AD", "User Name")
        c_ad_password = f.dec(cparser.get("AD", "Password"))
        c_ad_search_base = cparser.get("AD", "Search Base")
        c_attributes = ['sAMAccountName', 'mail', 'cn', 'userAccountControl', 'lastLogonTimestamp',
                        'whenCreated', 'whenChanged', 'userPrincipalName', 'distinguishedName']

        # Conexion y busqueda.
        print("+ Connect")
        try:
            server = Server(c_ad_server_name, get_info=ALL)
            conn = Connection(server, user=c_ad_user_name, password=c_ad_password, auto_bind=True)
            print("Conexión al AD exitosa")
        except Exception as err:
            print("Error en la conexión con el AD; Error: ", err)
            return
        print("- Connect")

        print("+ Search")
        search_key = '(&(mail=*'+self.txteMail.text().lower().strip()+'*))' if len(self.txteMail.text().strip()) > 0 else '(&(cn=*'+self.txtName.text().lower().strip()+'*))'
        conn.search(search_base=c_ad_search_base, search_filter=search_key, attributes=c_attributes, search_scope=SUBTREE)
        print("- Search")

        print("+ Agrega a pantalla")
        for i, row in enumerate(conn.entries):
            try:
                self.tblAD.insertRow(i)
                whencreated = f.formateaFecha(str(row.whencreated), '%Y-%m-%d %H:%M:%S%z', '%d/%m/%Y')
                whenchanged = f.formateaFecha(str(row.whenchanged), '%Y-%m-%d %H:%M:%S%z', '%d/%m/%Y')
                lastlogontimestamp = f.formateaFecha(str(row.lastlogontimestamp), '%Y-%m-%d %H:%M:%S.%f%z', '%d/%m/%Y %H:%M')
                useraccountcontrol = f.getADStatusCode(row.useraccountcontrol)

                # Busco la OU dentro del distinguishedName, para obtener el Country.
                print("+ Country")
                try:
                    dn = str(row.distinguishedName)
                    start = dn.find("DC=") + 3
                    end = dn.find(",", start)
                    d_country = dn[start:end].upper()
                except Exception as err:
                    d_country = " "
                print("- Country =", d_country)

                self.tblAD.setItem(i, 0, QTableWidgetItem(str(row.samaccountname)))
                self.tblAD.setItem(i, 1, QTableWidgetItem(str(row.mail)))
                self.tblAD.setItem(i, 2, QTableWidgetItem(str(row.cn)))
                self.tblAD.setItem(i, 3, QTableWidgetItem(useraccountcontrol))
                self.tblAD.setItem(i, 4, QTableWidgetItem(lastlogontimestamp))
                self.tblAD.setItem(i, 5, QTableWidgetItem(whencreated))
                self.tblAD.setItem(i, 6, QTableWidgetItem(whenchanged))
                self.tblAD.setItem(i, 7, QTableWidgetItem(d_country))
                self.tblAD.setItem(i, 8, QTableWidgetItem(str(row.userprincipalname)))
                self.tblAD.setItem(i, 9, QTableWidgetItem(str(row.distinguishedname)))

            except LDAPCursorError:
                self.tableWidget.setItem(i, 0, "Error al leer AD")
        print("- Agrega a pantalla")

        # Resize to content.
        self.tblAD.setSizeAdjustPolicy(QtWidgets.QAbstractScrollArea.AdjustToContents)
        self.tblAD.resizeColumnsToContents()
        print("/======= FIN AD =======/")


    def getOracle(self):
        print("/======= INICIO ORACLE =======/")

        # Conexión.
        ora = co.getOracle()
        conn = ora.conn
        cursor = conn.cursor()

        # sql = "SELECT NVL(fu.user_name, ' ') user_name, pp.full_name, pp.email_address" \
        #       "      ,CASE " \
        #       "         WHEN fu.user_id IS NULL THEN 'UNASSIGNED' " \
        #       "         WHEN (TO_CHAR(fu.start_date, 'MM/DD/YYYY HH24:MI') = TO_CHAR(TO_DATE('1','j'), 'MM/DD/YYYY HH24:MI') " \
        #       "          AND TO_CHAR(fu.end_date, 'MM/DD/YYYY HH24:MI') = TO_CHAR(TO_DATE('1','j'), 'MM/DD/YYYY HH24:MI')) THEN 'PENDING' " \
        #       "         WHEN fu.encrypted_user_password = 'INVALID' THEN 'LOCKED' " \
        #       "         WHEN fu.encrypted_user_password = 'EXTERNAL' THEN 'EXTERNAL' " \
        #       "         WHEN (fu.start_date IS NOT NULL AND fu.start_date <= SYSDATE) " \
        #       "          AND (fu.end_date IS NULL OR fu.end_date > SYSDATE) THEN 'ACTIVE' " \
        #       "         ELSE 'INACTIVE' " \
        #       "       END status " \
        #       "      ,(SELECT MAX(pps.date_start) FROM per_periods_of_service pps WHERE pps.person_id = pp.person_id) hire_date" \
        #       "      ,trunc(fu.end_date) end_date " \
        #       "      ,fu.last_logon_date " \
        #       "      ,(SELECT NAME FROM gl_ledgers WHERE ledger_id = pa.set_of_books_id) country " \
        #       "      ,(SELECT sup.full_name FROM per_people_f sup WHERE sup.person_id = pa.supervisor_id" \
        #       "           AND SYSDATE BETWEEN sup.effective_start_date AND sup.effective_end_date) supervisor" \
        #       "      ,(SELECT ss.full_name FROM per_people_f ss " \
        #       "         WHERE SYSDATE BETWEEN ss.effective_start_date AND ss.effective_end_date" \
        #       "           AND ss.person_id = (SELECT supervisor_id FROM per_assignments_f ps " \
        #       "                                WHERE SYSDATE BETWEEN ps.effective_start_date AND ps.effective_end_date" \
        #       "                                  AND person_id = pa.supervisor_id)) super_supervisor " \
        #       "      ,(SELECT COUNT(1) FROM fnd_user_resp_groups_direct furg " \
        #       "         WHERE furg.user_id = fu.user_id " \
        #       "           AND NVL(furg.end_date,SYSDATE) > TRUNC(SYSDATE) ) k_resp " \
        #       "      ,pp.employee_number " \
        #       "      /*,pp.date_of_birth birth_date*/ " \
        #       "      ,(SELECT concatenated_segments FROM gl_code_combinations_kfv WHERE code_combination_id = pa.default_code_comb_id) adi" \
        #       "      /*,NVL(pp.attribute3, ' ') approval_auth*/ " \
        #       "      ,NVL(pp.attribute2, ' ') worker_number " \
        #       "  FROM per_assignments_f  pa" \
        #       "      ,per_people_f       pp" \
        #       "      ,fnd_user           fu"

        sql = "SELECT NVL(fu.user_name, ' ') user_name, pp.full_name, pp.email_address " \
              "      ,CASE " \
              "         WHEN fu.user_id IS NULL THEN 'UNASSIGNED' " \
              "         WHEN (TO_CHAR(fu.start_date, 'MM/DD/YYYY HH24:MI') = TO_CHAR(TO_DATE('1','j'), 'MM/DD/YYYY HH24:MI') AND " \
              "               TO_CHAR(fu.end_date, 'MM/DD/YYYY HH24:MI') = TO_CHAR(TO_DATE('1','j'), 'MM/DD/YYYY HH24:MI')) THEN 'PENDING' " \
              "         WHEN fu.encrypted_user_password = 'INVALID' THEN 'LOCKED' " \
              "         WHEN fu.encrypted_user_password = 'EXTERNAL' THEN 'EXTERNAL' " \
              "         WHEN (fu.start_date IS NOT NULL AND fu.start_date <= SYSDATE) AND " \
              "              (fu.end_date IS NULL OR fu.end_date > SYSDATE) THEN 'ACTIVE' " \
              "         ELSE 'INACTIVE' " \
              "       END status " \
              "      ,(SELECT MAX(pps.date_start) FROM per_periods_of_service pps WHERE pps.person_id = pp.person_id) hire_date " \
              "      ,TRUNC(fu.end_date) end_date " \
              "      ,fu.last_logon_date " \
              "      ,(SELECT NAME FROM gl_ledgers WHERE ledger_id = pa.set_of_books_id) country " \
              "      ,(SELECT sup.full_name FROM per_people_f sup " \
              "         WHERE sup.person_id = pa.supervisor_id AND SYSDATE BETWEEN sup.effective_start_date AND sup.effective_end_date) supervisor " \
              "      ,(SELECT ss.full_name FROM per_people_f ss " \
              "         WHERE SYSDATE BETWEEN ss.effective_start_date AND ss.effective_end_date " \
              "           AND ss.person_id = (SELECT supervisor_id FROM per_assignments_f ps " \
              "                                WHERE SYSDATE BETWEEN ps.effective_start_date AND ps.effective_end_date " \
              "                                  AND person_id = pa.supervisor_id)) super_supervisor " \
              "      ,(SELECT COUNT(1) FROM fnd_user_resp_groups_direct furg " \
              "         WHERE furg.user_id = fu.user_id " \
              "          AND NVL(furg.end_date,SYSDATE) > TRUNC(SYSDATE) ) k_resp " \
              "      ,pp.employee_number " \
              "      ,(SELECT concatenated_segments FROM gl_code_combinations_kfv WHERE code_combination_id = pa.default_code_comb_id) adi " \
              "      ,NVL(pp.attribute2, ' ') worker_number " \
              "  FROM (SELECT * FROM per_assignments_f WHERE SYSDATE BETWEEN effective_start_date AND effective_end_date) pa " \
              "      ,(SELECT * FROM per_people_f WHERE SYSDATE BETWEEN effective_start_date AND effective_end_date) pp " \
              "      ,fnd_user           fu " \
              " WHERE pp.person_id(+) = fu.employee_id " \
              "   AND pa.person_id(+) = fu.employee_id " \
              "   AND fu.employee_id IS NOT NULL " \
              "   AND NOT EXISTS (SELECT 1 FROM fnd_lookup_values " \
              "                    WHERE lookup_type = 'XX_RESERVED_USERS' " \
              "                      AND LANGUAGE = 'US' " \
              "                      AND lookup_code = fu.user_name " \
              "                      AND enabled_flag = 'Y' " \
              "                      AND NVL(end_date_active,SYSDATE) > TRUNC(SYSDATE) ) "
        if len(self.txteMail.text().strip()) > 0:
            sql += " AND LOWER(pp.email_address) LIKE '%"+self.txteMail.text().lower().strip()+"%'"
        elif len(self.txtName.text().strip()) > 0:
            sql += " AND LOWER(pp.full_name) LIKE '%"+self.txtName.text().lower().strip()+"%'"
        else:
            sql += " AND fu.user_name = '!'"
        sql+= "UNION " \
              "SELECT NVL(fu.user_name, ' ') user_name, fu.description, fu.email_address " \
              "      ,CASE " \
              "         WHEN fu.user_id IS NULL THEN 'UNASSIGNED' " \
              "         WHEN (TO_CHAR(fu.start_date, 'MM/DD/YYYY HH24:MI') = TO_CHAR(TO_DATE('1','j'), 'MM/DD/YYYY HH24:MI') AND " \
              "               TO_CHAR(fu.end_date, 'MM/DD/YYYY HH24:MI') = TO_CHAR(TO_DATE('1','j'), 'MM/DD/YYYY HH24:MI')) THEN 'PENDING' " \
              "         WHEN fu.encrypted_user_password = 'INVALID' THEN 'LOCKED' " \
              "         WHEN fu.encrypted_user_password = 'EXTERNAL' THEN 'EXTERNAL' " \
              "         WHEN (fu.start_date IS NOT NULL AND fu.start_date <= SYSDATE) AND " \
              "              (fu.end_date IS NULL OR fu.end_date > SYSDATE) THEN 'ACTIVE' " \
              "         ELSE 'INACTIVE' " \
              "       END status " \
              "      ,fu.start_date hire_date " \
              "      ,TRUNC(fu.end_date) end_date " \
              "      ,fu.last_logon_date " \
              "      ,'' country " \
              "      ,'' supervisor " \
              "      ,'' super_supervisor " \
              "      ,(SELECT COUNT(1) FROM fnd_user_resp_groups_direct furg " \
              "         WHERE furg.user_id = fu.user_id " \
              "           AND NVL(furg.end_date,SYSDATE) > TRUNC(SYSDATE) ) k_resp " \
              "      ,'' employee_number " \
              "      ,'' adi " \
              "      ,'' worker_number " \
              "  FROM fnd_user           fu " \
              " WHERE fu.employee_id IS NULL " \
              "   AND NOT EXISTS (SELECT 1 FROM fnd_lookup_values " \
              "                    WHERE lookup_type = 'XX_RESERVED_USERS' " \
              "                      AND LANGUAGE = 'US' " \
              "                      AND lookup_code = fu.user_name " \
              "                      AND enabled_flag = 'Y' " \
              "                      AND NVL(end_date_active,SYSDATE) > TRUNC(SYSDATE) ) "
        if len(self.txteMail.text().strip()) > 0:
            sql += " AND LOWER(fu.email_address) LIKE '%"+self.txteMail.text().lower().strip()+"%'"
        elif len(self.txtName.text().strip()) > 0:
            sql += " AND LOWER(fu.full_name) LIKE '%"+self.txtName.text().lower().strip()+"%'"
        else:
            sql += " AND fu.user_name = '!'"
        print(sql)
        cursor.execute(sql)

        df = pd.read_sql(sql, conn)
        print(" "); print("df: "); print(df)

        # Ciero la conexión.
        conn.close()

        # Convierto los strings en fechas con el formato dd/mm/yyyy (y elimino los NaT).
        cols = ["HIRE_DATE", "END_DATE", "LAST_LOGON_DATE"] #, "BIRTH_DATE"]
        for c in range(0, len(cols)):
            df[cols[c]] = df[cols[c]].apply(lambda x: f.formateaFecha(str(x), '%Y-%m-%d %H:%M:%S', '%d/%m/%Y'))

        # Limpio los nulos.
        df.fillna("", inplace=True)

        # ===============================
        # CARGO REGISTROS EN PANTALLA.
        # ===============================
        borg = f.Borg()
        borg.df = df
        borg.df2 = df
        f.showTable(self=self, isChecked=False, qtable=self.tblOracle)
        print("/======= FIN ORACLE =======/")


    def getOpecus(self):
        print("/======= INICIO OPECUS =======/")

        opq = co.getOpecus()
        connection = opq.conn_opq

        sql = "select username, email, concat(lastname,', ',name) full_name, samaccountname, nombre, country_code" \
              "      ,status, admission_date, discharge_date, employed_number, supervisor_complete_name " \
              "      ,supervisor_email, birthdate, deparment_description, employed_position, if(zone_pci=1, 'Si', '  ') zone_pci, origen" \
              "  from idm_view "
        if len(self.txteMail.text().strip()) > 0:
            sql += " where lower(email) like '%" + self.txteMail.text().lower().strip() + "%'"
        elif len(self.txtName.text().strip()) > 0:
            sql += " where lower(concat(lastname,', ',name)) like '%" + self.txtName.text().lower().strip() + "%'"
        else:
            sql += " where lower(email) = '!'"
        print(sql)
        mycursor = connection.cursor()
        print("+ Ejecuta sql")
        mycursor.execute(sql)
        print("- Ejecuta sql")
        records = mycursor.fetchall()

        print("+ Carga registros en tblOpecus")
        for i, row in enumerate(records):
            type_fixed_row = tuple([el.decode('utf-8') if type(el) is bytearray else el for el in row])
            self.tblOpecus.insertRow(i)
            for j in range(0, len(type_fixed_row)):
                self.tblOpecus.setItem(i, j, QTableWidgetItem(str(type_fixed_row[j])))
        print("- Carga registros en tblOpecus")

        # Resize to content.
        self.tblOpecus.setSizeAdjustPolicy(QtWidgets.QAbstractScrollArea.AdjustToContents)
        self.tblOpecus.resizeColumnsToContents()

        connection.close()
        print("/======= FIN OPECUS =======/")


    def getCSV(self):
        print("======= INICIO CSV =======")

        cparser = ConfigParser()  # crea el objeto ConfigParser
        cparser.read("params/users.conf")
        c_csv_path = cparser.get("RRHH", "File Location") + "/RRHH.csv"
        csv = pd.read_csv(c_csv_path)
        df = pd.DataFrame(csv)
        print(" "); print("df: "); print(df)

        # Elimino los legajos duplicados.
        df.sort_values(['EMAIL_ADDRESS', 'TERMINATION_DATE'], ascending=[True, True], na_position='first', inplace=True)
        df.drop_duplicates(subset="EMAIL_ADDRESS", keep='first', inplace=True)

        # Genero la columna "FULL_NAME" y dejo el mail en lowercase.
        df["FULL_NAME"] = df["APELLIDO"] + ", " + df["PRIMER_NOMBRE"] + df["SEGUNDO_NOMBRE"].apply(lambda x: " " + x if x is None else "")
        df["STATUS"] = df["TERMINATION_DATE"].apply(lambda x: "Activo" if pd.isna(x) else "Inactivo" )
        df["ADI"] = df["CODIGO_ADI"].apply(lambda x: "   " if pd.isna(x) is None else str(x)) + "." + df["CENTRO_DE_COSTOS"]
        df["EMAIL_ADDRESS"] = df["EMAIL_ADDRESS"].str.lower()

        print("+ Filtro")
        if len(self.txteMail.text().strip()) > 0:
            print("Busca por mail:", self.txteMail.text().strip())
            df_found = df[df["EMAIL_ADDRESS"].str.contains(self.txteMail.text().strip(), na=False, case=False)]
        elif len(self.txtName.text().strip()) > 0:
            print("Busca por Nombre:", self.txtName.text().strip())
            df_found = df[df["FULL_NAME"].str.contains(self.txtName.text().strip(), na=False, case=False)]
        else:
            df_found = df[df["EMAIL_ADDRESS"].str.contains("!", case=False)]

        print("- Filtro")
        print(df_found)

        print(" "); print("+ Relleno los nulos")
        df_found = df_found.fillna("")
        print("- Relleno los nulos")

        # Dejo sólo las columnas que me importan y ordenadas para que vayan a la pantalla.
        df_found = df_found[["FULL_NAME", "EMAIL_ADDRESS", "TERRITORY", "STATUS", "MANAGER_NAME", "MANAGER_EMAIL_ADDRESS", "ENTERPRISE_HIRE_DATE",
                             "TERMINATION_DATE", "PERSON_NUMBER", "DEPARTAMENTO", "PUESTO", "JOB_FAMILY", "ADI", "AREA_PCI", "LOCATION",
                             "FECHA_DE_NACIMIENTO", "APPROVAL_AUTHORITY", "WORKER_NUMBER", "TIPO_DE_CAMBIO"]]

        # ===============================
        # CARGO REGISTROS EN PANTALLA.
        # ===============================
        borg = f.Borg()
        borg.df = df_found
        borg.df2 = df_found
        f.showTable(self=self, isChecked=False, qtable=self.tblCSV)

        print("======= FIN CSV =======")


    def getVPN(self, df):
        print("+ Filtro")
        if len(self.txteMail.text().strip()) > 0:
            print("Busca por mail:", self.txteMail.text().strip())
            df_found = df[df["EMAIL_ADDRESS"].str.contains(self.txteMail.text().strip(), case=False, na=False)]
        elif len(self.txtName.text().strip()) > 0:
            print("Busca por Nombre:", self.txtName.text().strip())
            df_found = df[df["VPN_FULL_NAME"].str.contains(self.txtName.text().strip(), case=False, na=False)]
        else:
            df_found = df[df["Email"].str.contains("!", case=False, na=False)]
        print("- Filtro")
        print("df_found: "); print(df_found)

        df_found = df_found.fillna("")
        df_found["VPN_STATUS"] = df_found["VPN_STATUS"].apply(lambda x: "Activo" if x else "Inactivo")

        print("+ Cargo registros en la tabla")
        for i in range(0, len(df_found.index)):
            self.tblVPN.insertRow(i)
            self.tblVPN.setItem(i, 0, QTableWidgetItem(df_found.iloc[i, df_found.columns.get_loc("VPN_USER_NAME")]))
            self.tblVPN.setItem(i, 1, QTableWidgetItem(df_found.iloc[i, df_found.columns.get_loc("EMAIL_ADDRESS")]))
            self.tblVPN.setItem(i, 2, QTableWidgetItem(df_found.iloc[i, df_found.columns.get_loc("VPN_FULL_NAME")]))
            self.tblVPN.setItem(i, 3, QTableWidgetItem(df_found.iloc[i, df_found.columns.get_loc("VPN_LAST_LOGON_DATE")]))
            self.tblVPN.setItem(i, 4, QTableWidgetItem(df_found.iloc[i, df_found.columns.get_loc("VPN_STATUS")]))
        print("- Cargo registros en la tabla")

        # Esto lo repito; no sé porqué pero si lo dejo sólo 1 vez no lo hace.
        self.tblVPN.setSizeAdjustPolicy(QtWidgets.QAbstractScrollArea.AdjustToContents)
        self.tblVPN.resizeColumnsToContents()
        print("======= FIN VPN =======")
        self.tblVPN.setSizeAdjustPolicy(QtWidgets.QAbstractScrollArea.AdjustToContents)
        self.tblVPN.resizeColumnsToContents()


    def getSurge(self):
        # Recupera los datos de SurgeMail (trae todos)
        df = co.getSurgeMail()

        print("+ Filtro")
        if len(self.txteMail.text().strip()) > 0:
            print("Busca por mail:", self.txteMail.text().strip())
            df_found = df[df["EMAIL_ADDRESS"].str.contains(self.txteMail.text().strip(), case=False, na=False)]
        elif len(self.txtName.text().strip()) > 0:
            print("Busca por Nombre:", self.txtName.text().strip())
            df_found = df[df["SM_FULL_NAME"].str.contains(self.txtName.text().strip(), case=False, na=False)]
        else:
            df_found = df[df["EMAIL_ADDRESS"].str.contains("!", case=False)]

        # ===============================
        # CARGO REGISTROS EN PANTALLA.
        # ===============================
        borg = f.Borg()
        borg.df2 = borg.df = df_found
        f.showTable(self=self, isChecked=False, qtable=self.tblSurge)


    def getGMail(self):
        df = co.getGSuite()

        print("+ Filtro")
        if len(self.txteMail.text().strip()) > 0:
            print("Busca por mail:", self.txteMail.text().strip())
            df_found = df[df["EMAIL_ADDRESS"].str.contains(self.txteMail.text().strip(), case=False)]
        elif len(self.txtName.text().strip()) > 0:
            print("Busca por Nombre:", self.txtName.text().strip())
            df_found = df[df["GS_FULL_NAME"].str.contains(self.txtName.text().strip(), case=False)]
        else:
            df_found = df[df["EMAIL_ADDRESS"].str.contains("!", case=False)]

        print(" "); print("df_found:"); print(df_found)

        # ===============================
        # CARGO REGISTROS EN PANTALLA.
        # ===============================
        borg = f.Borg()
        borg.df2 = borg.df = df_found
        f.showTable(self=self, isChecked=False, qtable=self.tblGMail)
