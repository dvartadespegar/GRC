import logging
import pandas as pd
import cx_Oracle
import base64
import mysql.connector      # se debe instalar con esto: pip install mysql-connector-python
import funcs as f


def get_SOD(filename):
    logging.info("  + Load SOX Matrix")

    sod_filename = filename #'Updates matriz SOD v.11.xlsx'
    df_sod_comb = pd.read_excel(sod_filename, sheet_name="Combinaciones")
    df_sod_resp = pd.read_excel(sod_filename, sheet_name="RolesxFuncion")

    print(" "); print("df_sod_comb (combinaciones - original):"); print(df_sod_comb)
    print(" "); print("df_sod_resp (roles x función):"); print(df_sod_resp)
    logging.info("  - Load SOX Matrix")

    print("  + Cambio los nombres de la fila de cabecera con los datos de la 1a fila")
    cols = []
    for i in range(0, len(df_sod_comb.columns)):
        if pd.isna(df_sod_comb.iloc[0, i]):
            cols.append("COL-"+str(i))
        else:
            cols.append(df_sod_comb.iloc[0, i])
    # print(cols)
    df_sod_comb.columns = cols
    print("  - Cambio los nombres de la fila de cabecera con los datos de la 1a fila")

    print("  + Borro la 1a fila y la 1a columna")
    df_sod_comb.drop([0], axis=0, inplace=True)                 # fila
    df_sod_comb.drop("COL-0", axis=1, inplace=True)             # columna (tiene que ir el nombre de la columna)
    df_sod_comb.rename(columns={"ID": "FUNC", "COL-1": "COD"}, inplace=True)
    print(" "); print("df_sod_comb (final)"); print(df_sod_comb)
    print("  - Borro la 1a fila y la 1a columna")

    # Toma de df_sod_comb donde están las "x" y va a buscar las respos que corresponden a esa combinación.
    # ------------------------------------------------------------------------------------------
    logging.info("  + Blender Process: multiply each combination with its associated roles")
    # ------------------------------------------------------------------------------------------
    res = []
    for i in range(0, len(df_sod_comb.index)):
        # print("i = ", str(i), " - ") #, df_sod_comb.loc[i,"ID"])
        for j in range(0, len(df_sod_comb.columns)):
            # logging.debug("  j = " + str(j))
            # print("  j = ", str(j), " - ") #, df_sod_comb.loc[j,"FUNC"] )
            if df_sod_comb.iloc[i, j] == "x":
                # logging.debug("combinación: (" + str(i) + "," + str(j) + ") - " + df_sod_comb.loc[i, "COD"])
                # print("combinación: (", str(i), ",", str(j), ")") #, df_sod_comb.loc[i, "COD"])
                print("combinación: (", df_sod_comb.iloc[i, 0], ",", df_sod_comb.columns[j], ")") #, df_sod_comb.loc[i, "COD"])
                r = df_sod_comb.iloc[i, 0]      # traigo el código de fila que tengo que ir a buscar a df_sod_resp
                c = df_sod_comb.columns[j]      # traigo el código de columna que tengo que ir a buscar a df_sod_resp
                #logging.debug("r = " + str(r) + "; c = " + str(c))
                #print("  r = ", str(r), "; c = " + str(c))

                df1 = df_sod_resp[df_sod_resp.loc[:,"COD"] == r]    # traigo todas las respos del código de la fila
                df2 = df_sod_resp[df_sod_resp.loc[:,"COD"] == c]    # traigo todas las respos del códido de la columna
                # print("df1 (r): (cant.reg. = ", str(len(df1.index)) + ")"); print(df1)
                # print("df2 (c): (cant.reg. = ", str(len(df2.index)) + ")"); print(df2)
                print("df1 (r) = ", df1.shape, "; df2 (c) = ", df2.shape)

                k = 0
                for x in range(0, len(df1.index)):
                    for y in range(0, len(df2.index)):
                        mod1 = df1.iloc[x, 4]    # "Aplicación"
                        resp1 = df1.iloc[x, 6]   # "Permiso / rol / WL"
                        appl1 = df1.iloc[x, 7]   # "Aplicación"
                        mod2 = df2.iloc[y, 4]    # "Aplicación"
                        resp2 = df2.iloc[y, 6]   # "Permiso / rol / WL"
                        appl2 = df2.iloc[y, 7]   # "Aplicación"

                        comb = str(r) + "-" + str(c)
                        # res.append([mod1, resp1, mod2, resp2, appl1, appl2, comb])
                        lista = [mod1, resp1, mod2, resp2, appl1, appl2, comb]
                        res.append(lista)
                        # print(lista)
                        k += 1
                print("Se insertaron ", str(k), " registros en 'res'")

    df_res = pd.DataFrame(res, columns=["MOD_A", "RESP_A", "MOD_B", "RESP_B", "APLIC_A", "APLIC_B", "COMBINACION"])  # .fillna("")
    print(" "); print("df_res (original): "); print(df_res)
    logging.info("  - Blender Process: multiply each combination with its associated roles; " + str(df_res.shape))

    # --------------------------------------------------------------------------------
    logging.info("  + Cleaning empty rows and incompatibilities with itself")
    # --------------------------------------------------------------------------------
    df_res.drop(df_res[df_res["RESP_A"] == df_res["RESP_B"]].index, inplace=True)

    df_res.dropna(axis=0, inplace=True)
    df_res["MOD_A"] = df_res["MOD_A"].str.upper()
    df_res["RESP_A"] = df_res["RESP_A"].str.upper()
    df_res["MOD_B"] = df_res["MOD_B"].str.upper()
    df_res["RESP_B"] = df_res["RESP_B"].str.upper()
    df_res["APLIC_A"] = df_res["APLIC_A"].str.upper()
    df_res["APLIC_B"] = df_res["APLIC_B"].str.upper()

    df_res.rename(columns={"MOD_A": "MODULE_NAME", "RESP_A": "ROLE_NAME", "MOD_B": "INCOMP_MODULE_NAME", "RESP_B": "INCOMP_ROLE_NAME",
                           "APLIC_A": "APPL_NAME", "APLIC_B": "APPL_INCOMP_NAME", "COMBINACION": "COMBINATION"}, inplace=True)
    print(" "); print("df_res (final): "); print(df_res)
    logging.info("  - Cleaning empty rows and incompatibilities with itself; " + str(df_res.shape))

    # -----------------------------------------------------------
    print("  + Filtering Oracle, ATP1 & ATP3")
    # -----------------------------------------------------------
    df_res["APPL_NAME"] = df_res["APPL_NAME"].apply(lambda x: x if x == "ATP1" or x == "ATP3" or x == "ORACLE" else "BORRAR")
    df_res["APPL_INCOMP_NAME"] = df_res["APPL_INCOMP_NAME"].apply(lambda x: x if x == "ATP1" or x == "ATP3" or x == "ORACLE" else "BORRAR")
    df_res.drop(df_res[df_res["APPL_NAME"] == "BORRAR"].index, inplace=True)
    df_res.drop(df_res[df_res["APPL_INCOMP_NAME"] == "BORRAR"].index, inplace=True)
    print("  - Filtering Oracle, ATP1 & ATP3; " + str(df_res.shape))

    return df_res


class getOracle():
    def __init__(self):
        df_params = f.get_params(key="ORACLE_EBS")
        # print(" "); print("df_params:"); print(df_params)

        # print("+ Recupera parámetros")
        for i in range(0, len(df_params.index)):
            globals()[str(df_params.loc[i, "Variable_Name"]).lower()] = df_params.loc[i, "Value"]
        passw = base64.b64decode(l_pwd).decode("utf-8")
        # print("- Recupera parámetros")

        # Conexión a Oracle.
        print("+ Conexión a Oracle")
        try:
            self.conn = cx_Oracle.connect(l_user_name, passw, l_host + "/" + l_sid)
            self.cur = self.conn.cursor()
            print("  Conexión ORACLE exitosa")
        except Exception as err:
            print("Error en la conexión a ORACLE; Error: ", err)
            return
        print("- Conexión a Oracle")

        # Hago los seteos de la sesión.
        self.conn.current_schema = "APPS"

    def get_full_user_resp(self):
        sql = "SELECT EMAIL_ADDRESS, USER_NAME, APPL_NAME, ROLE_NAME, NVL(NVL(country, profile_country),module) ROLE_COUNTRY" \
            "        ,/*MODULE*/ 'ORACLE' MODULE_NAME, EMPLOYEE_COUNTRY, SUPERVISOR, SUPER_SUPERVISOR, USER_ID, APPL_ID, ROLE_ID " \
            "  FROM  " \
            "( " \
            "SELECT EMAIL_ADDRESS, USER_NAME, APPL_NAME, ROLE_NAME, USER_ID, APPL_ID, ROLE_ID " \
            "      ,SUBSTR(ROLE_NAME,1,INSTR(ROLE_NAME,' ')-1) org_name " \
            "      ,TRIM(REGEXP_SUBSTR(ROLE_NAME,cs)) country " \
            "      ,CASE " \
            "         WHEN INSTR(ROLE_NAME,' CONC ') = 0 THEN " \
            "              TRIM(REGEXP_SUBSTR(ROLE_NAME,md,REGEXP_INSTR(ROLE_NAME,cs)+1 + NVL(LENGTH(TRIM(REGEXP_SUBSTR(ROLE_NAME,cs))),0) )) " \
            "         ELSE 'CONC' " \
            "       END module " \
            "      ,(select gsb.name from gl_sets_of_books gsb where gsb.set_of_books_id = " \
            "               (select pp.set_of_books_id from per_assignments_f pp " \
            "                 where pp.person_id = x.employee_id and sysdate between pp.effective_start_date and pp.effective_end_date and rownum = 1)) employee_country " \
            "      ,(select pp.full_name from per_people_f pp where rownum = 1 " \
            "           and sysdate between pp.effective_start_date and pp.effective_end_date and pp.person_id in " \
            "               (select pa.supervisor_id from per_assignments_f pa where pa.person_id = x.employee_id " \
            "                   and sysdate between pa.effective_start_date and pa.effective_end_date)) supervisor " \
            "      ,(select ss.full_name from per_people_f ss where rownum = 1 " \
            "           and sysdate between ss.effective_start_date and ss.effective_end_date and ss.person_id = " \
            "               (select ps.supervisor_id from per_assignments_f ps where rownum = 1 " \
            "                   and sysdate between ps.effective_start_date and ps.effective_end_date and ps.person_id in " \
            "                       (select pa.supervisor_id from per_assignments_f pa where pa.person_id = x.employee_id " \
            "                           and sysdate between pa.effective_start_date and pa.effective_end_date))) super_supervisor " \
            "      ,(SELECT NVL( NVL( NVL( " \
            "                       (SELECT pv.profile_option_value country " \
            "                         FROM fnd_profile_options_vl pn " \
            "                             ,fnd_profile_option_values pv " \
            "                        WHERE 1=1 " \
            "                          AND pv.application_id = pn.application_id " \
            "                          AND pv.profile_option_id = pn.profile_option_id " \
            "                          AND pv.level_id = 10003 " \
            "                          AND pv.level_value = role_id " \
            "                          AND NVL(pv.level_value_application_id,0) = x.application_id " \
            "                          AND pn.profile_option_name = 'JGZZ_COUNTRY_CODE') " \
            "                       ,  " \
            "                       (SELECT hou.attribute6 country " \
            "                          FROM hr_organization_units hou " \
            "                              ,fnd_profile_options_vl pn " \
            "                              ,fnd_profile_option_values pv " \
            "                         WHERE 1=1 " \
            "                           AND pv.application_id = pn.application_id " \
            "                           AND pv.profile_option_id = pn.profile_option_id " \
            "                           AND pv.level_id = 10003 " \
            "                           AND pv.level_value = role_id " \
            "                           AND NVL(pv.level_value_application_id,0) = x.application_id " \
            "                           AND pn.profile_option_name = 'ORG_ID' " \
            "                           AND hou.organization_id = TO_NUMBER(pv.profile_option_value)) " \
            "                       ) " \
            "                , " \
            "                 (SELECT f1.description country " \
            "                    FROM fnd_flex_values_vl   f1 " \
            "                        ,fnd_id_flex_segments s1 " \
            "                   WHERE 1=1 " \
            "                     AND s1.id_flex_code = 'GL#' " \
            "                     AND s1.application_id = 101  " \
            "                     AND s1.application_column_name = 'SEGMENT1' " \
            "                     AND f1.flex_value_set_id = s1.flex_value_set_id " \
            "                     AND f1.flex_value = (SELECT gcc.segment1 organization_id " \
            "                                            FROM gl_code_combinations   gcc " \
            "                                                ,gl_ledgers             gl " \
            "                                                ,fnd_profile_options_vl pn " \
            "                                                ,fnd_profile_option_values pv " \
            "                                           WHERE 1=1 " \
            "                                             AND pv.application_id = pn.application_id " \
            "                                             AND pv.profile_option_id = pn.profile_option_id " \
            "                                             AND pv.level_id = 10003 " \
            "                                             AND pv.level_value = role_id " \
            "                                             AND NVL(pv.level_value_application_id,0) = x.application_id " \
            "                                             AND pn.profile_option_name = 'GL_SET_OF_BKS_ID' " \
            "                                             AND gl.ledger_id = pv.profile_option_value " \
            "                                             AND gcc.code_combination_id = gl.ret_earn_code_combination_id) " \
            "                     AND NVL(f1.enabled_flag,'N') = 'Y' " \
            "                     AND NVL(f1.end_date_active,TO_DATE('31124712','ddmmyyyy')) > TRUNC(SYSDATE) " \
            "                     AND EXISTS ( SELECT 1 " \
            "                                    FROM apps.gl_sets_of_books  sob " \
            "                                        ,apps.financials_system_params_all fsp " \
            "                                   WHERE sob.set_of_books_id = fsp.set_of_books_id " \
            "                                     AND sob.chart_of_accounts_id = s1.id_flex_num " \
            "                                     AND fsp.org_id = NVL(TO_NUMBER(USERENV('CLIENT_INFO')),fsp.org_id) ) " \
            "                                 ) " \
            "                 ) " \
            "            , " \
            "             '' " \
            "            ) " \
            "             FROM dual ) profile_country " \
            "    FROM (SELECT LOWER(fu.email_address) EMAIL_ADDRESS, fu.user_name USER_NAME, 'ORACLE' APPL_NAME, fu.employee_id " \
            "              ,UPPER(fr.responsibility_name) ROLE_NAME, fu.user_id USER_ID, '0' APPL_ID, fr.responsibility_id ROLE_ID, fr.application_id " \
            "              ,'( AR | BR | BR | UY TEC | UY HLD | UY RIV | UY BAD | UY OPER | UY | CL | PE | EC | CO SAS | CO | CO2 | CO3 | MX SERV | MX NOM | MX OPER | MX | CR | PA | ES | ES2 | ES3 | VE SOL | US BVI | US | BB | UK BVI | DE | VE | REGIONAL | REG | FIN )' cs " \
            "              ,'( XX | GL | PO | INV | OM | OIE | RI | CE | FA | AP | AR )' md " \
            "          FROM fnd_user fu " \
            "              ,fnd_responsibility_tl       fr " \
            "              ,fnd_user_resp_groups_direct furg " \
            "         WHERE 1=1 " \
            "           AND fu.user_id = furg.user_id " \
            "           AND fr.responsibility_id = furg.responsibility_id " \
            "           AND fr.application_id = furg.responsibility_application_id " \
            "           AND fr.LANGUAGE = 'US' " \
            "           AND NVL(furg.end_date,SYSDATE) >= TRUNC(SYSDATE) " \
            "           AND NVL(fu.end_date,SYSDATE) > TRUNC(SYSDATE) " \
            "           AND EXISTS ( SELECT 1 " \
            "                          FROM fnd_responsibility r " \
            "                         WHERE r.responsibility_id = fr.responsibility_id " \
            "                           AND NVL(r.end_date,SYSDATE) > TRUNC(SYSDATE) ) " \
            "           AND NOT EXISTS (SELECT 1 " \
            "                             FROM fnd_lookup_values flv " \
            "                            WHERE flv.lookup_type = 'XX_RESERVED_USERS' " \
            "                              AND flv.lookup_code = fu.user_name " \
            "                              AND flv.LANGUAGE = 'US' " \
            "                              AND flv.enabled_flag = 'Y' " \
            "                              AND NVL(flv.end_date_active, SYSDATE) > TRUNC(SYSDATE) ) " \
            "        UNION ALL " \
            "        SELECT LOWER(fu.email_address) EMAIL_ADDRESS, fu.user_name USER_NAME, 'ORACLE' APPL_NAME, fu.employee_id " \
            "              ,UPPER(fr.responsibility_name) ROLE_NAME, fu.user_id USER_ID, '0' APPL_ID, fr.responsibility_id ROLE_ID, fr.application_id " \
            "              ,'( AR | BR | BR | UY TEC | UY HLD | UY RIV | UY BAD | UY OPER | UY | CL | PE | EC | CO SAS | CO | CO2 | CO3 | MX SERV | MX NOM | MX OPER | MX | CR | PA | ES | ES2 | ES3 | VE SOL | US BVI | US | BB | UK BVI | DE | VE | REGIONAL | REG | FIN )' cs " \
            "              ,'( XX | GL | PO | INV | OM | OIE | RI | CE | FA | AP | AR )' md " \
            "          FROM fnd_user fu " \
            "              ,fnd_responsibility_tl         fr " \
            "              ,fnd_user_resp_groups_indirect furg " \
            "         WHERE 1=1 " \
            "           AND fu.user_id = furg.user_id " \
            "           AND fr.responsibility_id = furg.responsibility_id " \
            "           AND fr.application_id = furg.responsibility_application_id " \
            "           AND fr.LANGUAGE = 'US' " \
            "           AND NVL(furg.end_date,SYSDATE) >= TRUNC(SYSDATE) " \
            "           AND NVL(fu.end_date,SYSDATE) > TRUNC(SYSDATE) " \
            "           AND EXISTS ( SELECT 1 " \
            "                          FROM fnd_responsibility r " \
            "                          WHERE r.responsibility_id = fr.responsibility_id " \
            "                           AND NVL(r.end_date,SYSDATE) > TRUNC(SYSDATE) ) " \
            "           AND NOT EXISTS (SELECT 1 " \
            "                             FROM fnd_lookup_values flv " \
            "                            WHERE flv.lookup_type = 'XX_RESERVED_USERS' " \
            "                              AND flv.lookup_code = fu.user_name " \
            "                              AND flv.LANGUAGE = 'US' " \
            "                              AND flv.enabled_flag = 'Y' " \
            "                              AND NVL(flv.end_date_active, SYSDATE) > TRUNC(SYSDATE) ) " \
            "        ) x " \
            " ) y "
            # " where lower(y.email_address) like '%ecantoni%' "
        # print(sql)
        df_ora = pd.read_sql(sql, self.conn)
        self.conn.close()
        return df_ora


class getATP1():
    def __init__(self):
        df = f.get_params(key="ATP1")

        # Creo las variables dinámicamente.
        for i in range(0, len(df.index)):
            globals()[str(df.loc[i, "Variable_Name"]).lower()] = df.loc[i, "Value"]

        print("+ Conexión a ATP1")
        try:
            self.conn = mysql.connector.connect(host=l_host,
                                                port=l_port,
                                                database=l_database,
                                                user=l_user,
                                                password=f.dec(l_pwd))
            print("  Conexión ATP1 exitosa")
        except Exception as err:
            print("Error en la conexión a ATP1; ", err)
            return
        print("- Conexión a ATP1")

    def get_full_user_role(self):
        sql = "select DISTINCT LOWER(this_.us_email) as EMAIL_ADDRESS, cr1_.cr_username as USER_NAME " \
              "      ,'ATP1' as APPL_NAME, UPPER(aps.name) as MODULE_NAME, UPPER(SS.name) as ROLE_NAME " \
              "      ,this_.OID USER_ID, aps.OID as APPL_ID, SS.OID as ROLE_ID, 'ATP' as COUNTRY " \
              "  from atp.USERS this_ " \
              "  left outer join atp.ACCESSTOKEN accesstoke3_ on this_.OID = accesstoke3_.us_OID " \
              " inner join atp.CREDENTIALS cr1_ on this_.OID = cr1_.us_OID " \
              " inner join atp.USER_PERMISSIONS up on cr1_.us_OID = up.haveUser_OID " \
              " inner join atp.PERMISSIONS pp on pp.OID = up.OID " \
              " inner join atp.PERMISSION_TYPES ppp on pp.pt_OID = ppp.OID " \
              " inner join atp.SECURABLE_OBJECTS SS on pp.so_OID = SS.OID " \
              " inner join atp.APPLICATIONS aps on ppp.ap_OID = aps.OID " \
              " where accesstoke3_.OID IS NOT NULL " \
              "   AND cr1_.cr_enabled = 1 " \
              "   AND (this_.OID in (select user_.OID as y0_ " \
              "                        from atp.USERS user_ " \
              "                        left outer join atp.RD_COUNTRY cou9_ on user_.us_country = cou9_.OID " \
              "                        left outer join atp.USER_HIRARCHY subordinat12_ on user_.OID = subordinat12_.CHIEF_OID " \
              "                        left outer join atp.USERS sub3_ on subordinat12_.SUBORDINATE_OID = sub3_.OID " \
              "                        left outer join atp.RD_COUNTRY subcou6_ on sub3_.us_country = subcou6_.OID " \
              "                        left outer join atp.USER_TAG tags15_ on sub3_.OID = tags15_.USER_OID " \
              "                        left outer join atp.TAGS subtg7_ on tags15_.TAG_OID = subtg7_.OID " \
              "                        left outer join atp.ACCESSTOKEN subat5_ on sub3_.OID = subat5_.us_OID " \
              "                        left outer join atp.CREDENTIALS subcr4_ on sub3_.OID = subcr4_.us_OID " \
              "                        left outer join atp.USER_TAG tags19_ on user_.OID = tags19_.USER_OID " \
              "                        left outer join atp.TAGS tg8_ on tags19_.TAG_OID = tg8_.OID " \
              "                        left outer join atp.ACCESSTOKEN at2_ on user_.OID = at2_.us_OID " \
              "                        inner join atp.CREDENTIALS cr1_ on user_.OID = cr1_.us_OID) " \
              "       ) "
              # " and this_.us_email like '%ecantoni%' "
        #print(sql)
        df_atp1 = pd.read_sql(sql, self.conn)
        # Corrijo el user_name que sale como una lista de números [192 111 237 234 ....]
        df_atp1["USER_NAME"] = df_atp1["USER_NAME"].apply(lambda x: x.decode('utf-8') if type(x) is bytearray else x)
        self.conn.close()

        return df_atp1


class getATP3():
    def __init__(self):
        df = f.get_params(key="ATP3")

        # Creo las variables dinámicamente.
        for i in range(0, len(df.index)):
            globals()[str(df.loc[i, "Variable_Name"]).lower()] = df.loc[i, "Value"]

        print("+ Conexión a ATP3")
        try:
            self.conn = mysql.connector.connect(host=l_host,
                                                port=l_port,
                                                database=l_database,
                                                user=l_user,
                                                password=f.dec(l_pwd))
            print("  Conexión ATP3 exitosa")
        except Exception as err:
            print("Error en la conexión a ATP3; ", err)
            return
        print("- Conexión a ATP3")

    def get_full_user_role(self):
        sql = "SELECT DISTINCT LOWER(atp.user.email) EMAIL_ADDRESS, atp.user.username USER_NAME " \
              "      ,'ATP3' APPL_NAME, UPPER(atp.application.name) MODULE_NAME, UPPER(atp.role.name) ROLE_NAME " \
              "      ,atp.user.id USER_ID, atp.application.id APPL_ID, atp.role.id ROLE_ID, 'ATP' COUNTRY " \
              "  FROM atp.user " \
              "  JOIN atp.user_assigned_role ON atp.user.id = atp.user_assigned_role.user_id " \
              "  JOIN atp.assigned_role ON atp.user_assigned_role.assigned_role_id = atp.assigned_role.id " \
              "  JOIN atp.role ON atp.assigned_role.role_id = atp.role.id " \
              "  JOIN atp.application ON atp.role.application_id = atp.application.id " \
              "  RIGHT JOIN atp.user_application ON atp.user.id = atp.user_application.user_id " \
              " WHERE (atp.user.deleted = 0) and (atp.user.active = 1) and (atp.user.source = 'AD') "
              # "and atp.user.email like '%ecantoni%' "
        # print(sql)
        df_atp3 = pd.read_sql(sql, self.conn)
        self.conn.close()

        return df_atp3


def getCSV():
    print("+ Connect CSV")
    df = f.get_params(key="RRHH")

    # Creo las variables dinámicamente.
    for i in range(0, len(df.index)):
        globals()[str(df.loc[i, "Variable_Name"]).lower()] = df.loc[i, "Value"]

    c_csv_path = l_path + "/RRHH.csv"
    print("- Connect CSV; path = ", c_csv_path)
    csv = pd.read_csv(c_csv_path)
    df_csv = pd.DataFrame(csv)

    # Elimino los legajos duplicados.
    df_csv.sort_values(['EMAIL_ADDRESS','TERMINATION_DATE'], ascending=[True, True], na_position='first', inplace=True)
    df_csv.drop_duplicates(subset="EMAIL_ADDRESS", keep='first', inplace=True)

    # Genero la columna "FULL_NAME" y dejo el mail en lowercase.
    df_csv["FULL_NAME"] = df_csv["APELLIDO"] + ", " + df_csv["PRIMER_NOMBRE"] + df_csv["SEGUNDO_NOMBRE"].apply(lambda x: " "+x if x is None else "")
    df_csv["EMAIL_ADDRESS"] = df_csv["EMAIL_ADDRESS"].str.lower()

    # Genero la columna "SUPER_SUPERV_EMAIL" y "SUPER_SUPERV_NAME" (OJO! esto entrega una lista que puede estar vacía)
    df_csv["SUPER_SUPERV_EMAIL"] = df_csv["MANAGER_EMAIL_ADDRESS"].apply(lambda x: df_csv[df_csv["EMAIL_ADDRESS"] == x]["MANAGER_EMAIL_ADDRESS"].to_list())
    df_csv["SUPER_SUPERV_EMAIL"] = df_csv["SUPER_SUPERV_EMAIL"].apply(lambda x: "" if len(x) == 0 else x[0])
    df_csv["SUPER_SUPERV_NAME"] = df_csv["MANAGER_EMAIL_ADDRESS"].apply(lambda x: df_csv[df_csv["EMAIL_ADDRESS"] == x]["MANAGER_NAME"].to_list())
    df_csv["SUPER_SUPERV_NAME"] = df_csv["SUPER_SUPERV_NAME"].apply(lambda x: "" if len(x) == 0 else x[0])

    # Dejo sólo las columnas que me interesan, y las renombro.
    df_csv = df_csv[["FULL_NAME", "EMAIL_ADDRESS", "TERRITORY", "MANAGER_NAME", "MANAGER_EMAIL_ADDRESS", "SUPER_SUPERV_NAME", "SUPER_SUPERV_EMAIL", "TERMINATION_DATE"]]
    df_csv.rename(columns={"TERRITORY": "COUNTRY_CODE", "MANAGER_NAME": "SUPERVISOR_NAME", "MANAGER_EMAIL_ADDRESS": "SUPERVISOR_EMAIL_ADDRESS", "TERMINATION_DATE": "END_DATE"}, inplace=True)

    return df_csv


