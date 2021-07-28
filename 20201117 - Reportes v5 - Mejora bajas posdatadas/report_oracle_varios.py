import pandas as pd
from datetime import datetime
import threading

import funcs as f
import connections as c


#pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


def reportApplUsers(self, envia_sftp=False):
    print("+ REPORT APPLICATION USERS")
    start = datetime.now()

    # ----------------------------------------------
    # Traigo el df de Oracle.
    # ----------------------------------------------
    print("+ Get Oracle Appl Users")
    oracle = c.getOracle()
    df = oracle.getApplUsers()
    print("- Get Oracle Appl Users")

    f.quick_excel(df, "Reporte_Oracle_Appl_Users", final=True, put_sftp=envia_sftp)

    print("Processing elapsed time {}".format(datetime.now() - start))  # esto muestra el tiempo de ejecución.
    print("- REPORT APPLICATION USERS")


def reportSuperUsers(self, envia_sftp=False):
    print("+ REPORT ORACLE SUPER USERS")
    start = datetime.now()

    # ----------------------------------------------
    # Traigo el df de Oracle.
    # ----------------------------------------------
    print("+ Get Oracle Super Users")
    oracle = c.getOracle()

    sql = "SELECT fu.user_name, fr.responsibility_name, pp.full_name " \
          "      ,NVL(fu.email_address,pp.email_address) email_address " \
          "      ,(SELECT NAME FROM gl_ledgers WHERE ledger_id = pa.set_of_books_id) country " \
          "      ,(SELECT sup.full_name FROM per_people_f sup WHERE sup.person_id = pa.supervisor_id" \
          "           AND SYSDATE BETWEEN sup.effective_start_date AND sup.effective_end_date) ora_supervisor_name" \
          "      ,(SELECT ss.full_name FROM per_people_f ss " \
          "         WHERE SYSDATE BETWEEN ss.effective_start_date AND ss.effective_end_date" \
          "           AND ss.person_id = (SELECT supervisor_id FROM per_assignments_f ps " \
          "                                WHERE SYSDATE BETWEEN ps.effective_start_date AND ps.effective_end_date" \
          "                                  AND person_id = pa.supervisor_id)) ora_super_supervisor " \
          "  FROM per_assignments_f pa" \
          "      ,per_people_f pp" \
          "      ,fnd_responsibility_tl fr" \
          "      ,fnd_user_resp_groups_direct furg" \
          "      ,fnd_user fu " \
          " WHERE furg.user_id = fu.user_id " \
          "   AND NVL(fu.end_date, SYSDATE) > TRUNC(SYSDATE) " \
          "   AND EXISTS(SELECT 1 FROM fnd_responsibility frx " \
          "               WHERE frx.responsibility_id = fr.responsibility_id " \
          "                 AND NVL(frx.end_date, SYSDATE) > TRUNC(SYSDATE)) " \
          "   AND NVL(furg.end_date, SYSDATE) > TRUNC(SYSDATE) " \
          "   AND fr.responsibility_id = furg.responsibility_id " \
          "   AND fr.LANGUAGE = 'US' " \
          "   AND pp.person_id(+) = fu.employee_id " \
          "   AND pa.person_id(+) = fu.employee_id " \
          "   AND SYSDATE BETWEEN pa.effective_start_date(+) AND pa.effective_end_date " \
          "   AND SYSDATE BETWEEN pp.effective_start_date(+) AND pp.effective_end_date " \
          "   /* AND fu.user_name NOT LIKE 'INTERFACE%' */ " \
          "   AND UPPER(fr.responsibility_name) LIKE'%SUPER%USER%' " \
          " ORDER BY fu.user_id, fr.responsibility_name"
    print(sql)

    # Ejecuto el query y devuelvo el df.
    df_ora = oracle.get_oracle(p_sql=sql)

    print("- Get Oracle Appl Users")

    f.quick_excel(df_ora, "Reporte_Oracle_Super_Users", final=True, put_sftp=envia_sftp)

    print("Processing elapsed time {}".format(datetime.now() - start))  # esto muestra el tiempo de ejecución.
    print("- REPORT ORACLE SUPER USERS")


def reportConfigurador(self, envia_sftp=False):
    print("+ REPORT ORACLE CONFIGURADOR")
    start = datetime.now()

    # ----------------------------------------------
    # Traigo el df de Oracle.
    # ----------------------------------------------
    print("+ Get Oracle Configurador")
    oracle = c.getOracle()

    sql = "SELECT fu.user_name, fr.responsibility_name, pp.full_name " \
          "      ,NVL(fu.email_address,pp.email_address) email_address " \
          "      ,(SELECT NAME FROM gl_ledgers WHERE ledger_id = pa.set_of_books_id) country " \
          "      ,(SELECT sup.full_name FROM per_people_f sup WHERE sup.person_id = pa.supervisor_id" \
          "           AND SYSDATE BETWEEN sup.effective_start_date AND sup.effective_end_date) ora_supervisor_name" \
          "      ,(SELECT ss.full_name FROM per_people_f ss " \
          "         WHERE SYSDATE BETWEEN ss.effective_start_date AND ss.effective_end_date" \
          "           AND ss.person_id = (SELECT supervisor_id FROM per_assignments_f ps " \
          "                                WHERE SYSDATE BETWEEN ps.effective_start_date AND ps.effective_end_date" \
          "                                  AND person_id = pa.supervisor_id)) ora_super_supervisor " \
          "  FROM per_assignments_f pa" \
          "      ,per_people_f pp" \
          "      ,fnd_responsibility_tl fr" \
          "      ,fnd_user_resp_groups_direct furg" \
          "      ,fnd_user fu " \
          " WHERE furg.user_id = fu.user_id " \
          "   AND NVL(fu.end_date, SYSDATE) > TRUNC(SYSDATE) " \
          "   AND EXISTS(SELECT 1 FROM fnd_responsibility frx " \
          "               WHERE frx.responsibility_id = fr.responsibility_id " \
          "                 AND NVL(frx.end_date, SYSDATE) > TRUNC(SYSDATE)) " \
          "   AND NVL(furg.end_date, SYSDATE) > TRUNC(SYSDATE) " \
          "   AND fr.responsibility_id = furg.responsibility_id " \
          "   AND fr.LANGUAGE = 'US' " \
          "   AND pp.person_id(+) = fu.employee_id " \
          "   AND pa.person_id(+) = fu.employee_id " \
          "   AND SYSDATE BETWEEN pa.effective_start_date(+) AND pa.effective_end_date " \
          "   AND SYSDATE BETWEEN pp.effective_start_date(+) AND pp.effective_end_date " \
          "   AND UPPER(fr.responsibility_name) LIKE'%CONFIGURADOR%' " \
          " ORDER BY fu.user_id, fr.responsibility_name"
    print(sql)

    # Ejecuto el query y devuelvo el df.
    df_ora = oracle.get_oracle(p_sql=sql)

    print("- Get Oracle Configurador")

    f.quick_excel(df_ora, "Reporte_Oracle_Configurador", final=True, put_sftp=envia_sftp)

    print("Processing elapsed time {}".format(datetime.now() - start))  # esto muestra el tiempo de ejecución.
    print("- REPORT ORACLE CONFIGURADOR")


def reportUsersCreation(self, envia_sftp=False):
    print("+ REPORT ORACLE USERS CREATION")
    start = datetime.now()

    # ----------------------------------------------
    # Traigo el df de Oracle.
    # ----------------------------------------------
    print("+ Get Users Creation")
    oracle = c.getOracle()

    sql = "SELECT fu.user_name, pp.full_name " \
          "      ,NVL(fu.email_address,pp.email_address) email_address " \
          "      ,(SELECT NAME FROM gl_ledgers WHERE ledger_id = pa.set_of_books_id) country " \
          "      ,(SELECT sup.full_name FROM per_people_f sup WHERE sup.person_id = pa.supervisor_id" \
          "           AND SYSDATE BETWEEN sup.effective_start_date AND sup.effective_end_date) ora_supervisor_name" \
          "      ,(SELECT ss.full_name FROM per_people_f ss " \
          "         WHERE SYSDATE BETWEEN ss.effective_start_date AND ss.effective_end_date" \
          "           AND ss.person_id = (SELECT supervisor_id FROM per_assignments_f ps " \
          "                                WHERE SYSDATE BETWEEN ps.effective_start_date AND ps.effective_end_date" \
          "                                  AND person_id = pa.supervisor_id)) ora_super_supervisor " \
          "      ,TO_CHAR(fu.creation_date,'DD/MM/YYYY HH24:MI') creation_date " \
          "      ,(SELECT fx.user_name FROM fnd_user fx WHERE fx.user_id = fu.created_by) created_by " \
          "      ,TO_CHAR(fu.last_update_date,'dd/mm/yyyy') last_update_date " \
          "      ,DECODE(fu.user_id, fu.last_updated_by, ' ',DECODE(fu.user_name,'GUEST',' ',fu.user_name)) last_updated_by " \
          "  FROM per_assignments_f pa" \
          "      ,per_people_f pp" \
          "      ,fnd_user fu " \
          " WHERE NVL(fu.end_date,SYSDATE) > TRUNC(SYSDATE)" \
          "   AND SYSDATE between pa.effective_start_date AND pa.effective_end_date" \
          "   AND SYSDATE between pp.effective_start_date AND pp.effective_end_date" \
          "   AND pa.person_id = pp.person_id" \
          "   AND pp.person_id(+) = fu.employee_id" \
          "   AND (TRUNC(SYSDATE) - TRUNC(fu.creation_date)) <= 180 " \
          " ORDER BY fu.creation_date DESC, fu.user_name ASC"
    print(sql)

    # Ejecuto el query y devuelvo el df.
    df_ora = oracle.get_oracle(p_sql=sql)

    print("- Get Oracle Users Creation")

    f.quick_excel(df_ora, "Reporte_Oracle_Users_Creation", final=True, put_sftp=envia_sftp)

    print("Processing elapsed time {}".format(datetime.now() - start))  # esto muestra el tiempo de ejecución.
    print("- REPORT ORACLE USERS CREATION")


def reportRespEquipoFST(self, envia_sftp=False):
    print("+ REPORT ORACLE RESP EQUIPO FST")
    start = datetime.now()

    # ----------------------------------------------
    # Traigo el df de Oracle.
    # ----------------------------------------------
    print("+ Get Resp Equipo FST")
    oracle = c.getOracle()

    sql =   "SELECT fu.user_name " \
            "      ,fr.responsibility_name " \
            "      ,he.full_name, NVL(he.email_address,fu.email_address) email_address " \
            "      ,TO_CHAR(TRUNC(furg.last_update_date),'dd/mm/yyyy') assign_date " \
            "  FROM per_people_f                he   " \
            "      ,fnd_responsibility_tl       fr   " \
            "      ,fnd_user_resp_groups_direct furg " \
            "      ,apps.fnd_user               fu   " \
            " WHERE 1=1 " \
            "   AND fr.responsibility_name NOT LIKE '%CONFIGURADOR%' AND UPPER(fr.responsibility_name) NOT LIKE '%CONSULTA%' " \
            "   AND fr.responsibility_name NOT LIKE '%RENDICION DE GASTOS%' " \
            "   AND fr.responsibility_name <> 'XX Customizaciones' AND fr.responsibility_name <> 'Application Diagnostics' " \
            "   AND furg.user_id = fu.user_id " \
            "   AND fr.responsibility_id = furg.responsibility_id " \
            "   AND fr.application_id = furg.responsibility_application_id " \
            "   AND he.person_id(+) = fu.employee_id " \
            "   AND EXISTS (SELECT 1 FROM fnd_responsibility frx " \
            "                WHERE frx.responsibility_id = fr.responsibility_id AND NVL(frx.end_date,SYSDATE) >= TRUNC(SYSDATE)) " \
            "   AND fr.language = 'US' " \
            "   AND NVL(furg.end_date,TO_DATE('31124712','ddmmyyyy')) >= TRUNC(SYSDATE) " \
            "   AND NVL(fu.end_date,TO_DATE('31124712','ddmmyyyy')) > TRUNC(SYSDATE) " \
            "   AND fu.user_name IN " \
            "(  " \
            " SELECT fu.user_name " \
            "   FROM fnd_user          fu " \
            "       ,per_assignments_f pa " \
            "  WHERE fu.employee_id(+) = pa.person_id " \
            "    AND NVL(fu.end_date,TO_DATE('31124712','ddmmyyyy')) > TRUNC(SYSDATE) " \
            "CONNECT BY PRIOR pa.person_id = DECODE( fu.user_name, 'DAMIAN.SCOKIN', TO_NUMBER(NULL), pa.supervisor_id ) " \
            "  START WITH fu.user_name = 'PCHARCHAFLIE' " \
            ") " \
            "  ORDER BY fu.user_name, fr.responsibility_name "
    print(sql)

    # Ejecuto el query y devuelvo el df.
    df_ora = oracle.get_oracle(p_sql=sql)

    print("- Get Resp Equipo FST")

    f.quick_excel(df_ora, "Reporte_Oracle_Resp_Equipo_FST", final=True, put_sftp=envia_sftp)

    print("Processing elapsed time {}".format(datetime.now() - start))  # esto muestra el tiempo de ejecución.
    print("- REPORT ORACLE RESP EQUIPO FST")
