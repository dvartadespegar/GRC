import pandas as pd
import numpy as np
import threading
from datetime import datetime
import funcs as f
import connections as c

#pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


def reportEndUsers(self, completo=False, envia_sftp=False, args=()):
    print(" "); print("+ REPORTE NÓMINA VS ORACLE")
    print("  completo =", completo, "; envia_sftp =", envia_sftp)
    print("  args =", args)
    start = datetime.now()

    df_ora = args[0]
    df_nomina = args[1]

    #
    # hilo = MiHilo(args=(completo, envia_sftp, ), daemon=False)
    # print("- MiHilo")
    # hilo.start()
    #
    # print("isAlive? (después del while) = ", hilo.is_alive())
#
#
# class MiHilo(threading.Thread):
#     def __init__(self, group=None, target=None, name=None, args=(), kwargs=None, *, daemon=None):
#         self.dt_start = datetime.datetime.now()
#         print("+ __init__ de MiHilo")
#         super().__init__(group=group, target=target, name=name, daemon=daemon)
#         print(args)
#         self.completo = args[0]     # chkCompleto.isChecked()
#         self.envia_sftp = args[1]   # envía al sftp
#         print("- __init__ de MiHilo")
#
#         self.prog = f.winbar()
#         self.prog.setTitle("Oracle - Reporte de Bajas (Completo)" if self.completo else "Oracle - Reporte de Bajas")
#         self.prog.show()
#
#     def run(self):
#         """ ESTO FUNCIONA OK!! El df4 tiene el mismo contenido que el df3.
#             Se descarta su uso porque hace lo mismo que el merge, y el merge lo hace en 1 sola línea y no necesita índices.
#
#         # Creo los índices para después poder joinear.
#         print("+ Indices")
#         df_filtrado.set_index('EMAIL_ADDRESS', inplace=True)
#         df_csv.set_index('EMAIL_ADDRESS', inplace=True)
#         print("- Indices")
#
#         # Joinea df con df_csv por el email_address, y agrega 2 columnas a df2.
#         df4 = df_filtrado.join(df_csv, how='left', lsuffix="_dff", rsuffix="_csv")
#         print(" "); print("df4 joineado: "); print(df4)
#
#         # Elimino los índices.
#         print("+ Elimina Indices")
#         df_filtrado.reset_index(drop=False, inplace=True)
#         df_csv.reset_index(drop=False, inplace=True)
#         print("- Elimina Indices")
#         """
#         self.prog.adv(5)

        # Traigo el df de Oracle.
        # oracle = c.getOracle()
        # df_ora = oracle.get_actives()

    df_ora = df_ora.drop(["ORIGEN"], axis=1)        # elimino la columna de origen para que no se choque con la columna origen del df de nómina.

        # self.prog.adv(20)

        # Traigo la nómina (RRHH + Opecus + AD)
        # df_nomina = c.nomina()
        # self.prog.adv(40)

    print("+ Merge")
    df = pd.merge(left=df_ora, right=df_nomina, how='left', left_on='EMAIL_ADDRESS', right_on='EMAIL_ADDRESS')
    print("- Merge")

    # Agrego al campo ORIGEN el valor AD, porque lo que vino del df_concat ya lo tiene, pero lo que vino del AD no.
    df.loc[pd.isna(df["ORIGEN"]), "ORIGEN"] = "ORACLE"

    print(" "); print("df (Merged): "); print(df)
    # self.prog.adv(50)
    f.quick_excel(df, "Rep_Oracle_Bajas (Merged)")

    # --------------------------------------------------------------------------------------
    # Completo los datos de aquellos registros que vinieron vacíos en el AD (que son bajas);
    # si no hago esto, un usuario de oracle activo pero inactivo en rrhh u opecus, no podría
    # tener su fecha de baja (porque al estar inactivo las búsquedas de nómina no lo traen).
    # --------------------------------------------------------------------------------------
    # De los registros que vinieron con el AD_SAMACCOUNTNAME en blanco (que es porque son cuentas deshabiltadas en AD), las voy a buscar al AD para ser más explícito en la salida del reporte.
    print("len(df.index) = ", len(df.index))
    print("+ Rellena valores vacíos del AD")
    ad = c.getAD()
    for i in range(0, len(df.index)):
        if pd.isna(df.loc[i, "AD_SAMACCOUNTNAME"]):
            detalle_ad = ad.get_samAccountName(df.loc[i, "EMAIL_ADDRESS"])
            df.loc[i, "AD_SAMACCOUNTNAME"] = detalle_ad[0]
            df.loc[i, "AD_LAST_LOGON_DATE"] = detalle_ad[1]
            df.loc[i, "AD_STATUS"] = detalle_ad[2]
    print("- Rellena valores vacíos del AD")
    # self.prog.adv(60)

    # De los registros que vinieron con status = "DISABLE" en el AD, voy a buscar las cuentas de Opecus, para ser más explícito en la salida del reporte.
    print("+ Rellena valores de Opecus")
    opq = c.getOpecus()
    for i in range(0, len(df.index)):
        if pd.isna(df.loc[i, "AD_STATUS"]) == False:
            if df.loc[i, "AD_STATUS"].lower().find("disable") >= 0:
                arr = opq.get_user(df.loc[i, "EMAIL_ADDRESS"])
                if pd.isna(arr[6]) == False and pd.isna(df.loc[i, "END_DATE"]) == True:
                    df.loc[i, "END_DATE"] = arr[6]  # discharge_date
    opq.close()
    print("- Rellena valores de Opecus")
    # self.prog.adv(70)

    print(" "); print("df (final):"); print(df); print(df.info())
    print( df[~pd.isna(df["END_DATE"])] )  # necesito ver el formato de los datos en el end_date
    f.quick_excel(df, "Rep_Oracle_Bajas (Final)")

    # ------------------------------------------------------------------------------------------------------
    # Agrego la columna de comentarios:
    # ------------------------------------------------------------------------------------------------------
    print("+ Cargo los comentarios")
    # for i in range(0, len(df.index)):
    #     if pd.isna(df.loc[i, "COMMENTS"]) and \
    #         pd.isna(df.loc[i, "END_DATE"]) == False and len(str(df.loc[i, "END_DATE"])) > 3 and \
    #         (pd.isna(df.loc[i, "ORA_END_DATE"]) == True or len(str(df.loc[i, "ORA_END_DATE"])) <= 3):
    #         df.loc[i, "COMMENTS"] = "Dar de Baja EBS"

    # Convierto la fecha en campo date, además la copio en otro campo para evitar el error de slice.

    df["NEW_END_DATE"] = df.loc[:, "END_DATE"]
    df['NEW_END_DATE'] = df["NEW_END_DATE"].apply(lambda x: "" if x == "" else pd.to_datetime(x, format='%d/%m/%Y'))

    df["COMMENTS"] = None
    cond =  [   # No está en RRHH ni en Opecus, y tiene Oracle Activo (sólo Oracle)
                # (pd.isna(df["COMMENTS"])) &
                (pd.isna(df["FULL_NAME"])) & (pd.isna(df["OPQ_FULL_NAME"])) & (df["ORA_STATUS"] == "ACTIVE"),
                # No está en RRHH, está en Opecus Desactivado y en Oracle Activo (sólo Opecus).
                # (pd.isna(df["COMMENTS"])) &
                (pd.isna(df["FULL_NAME"])) & (pd.isna(df["OPQ_FULL_NAME"]) == False) &
                (df["OPQ_STATUS"] == "Desactivado") & (df["ORA_STATUS"] == "ACTIVE"),
                # Está Inactivo en RRHH, no está en Opecus y tiene Oracle Activo (sólo RRHH).
                # (pd.isna(df["COMMENTS"])) &
                (pd.isna(df["END_DATE"]) == False) & (len(str(df["END_DATE"])) > 7) &
                (df["NEW_END_DATE"] < datetime.now()) &
                (pd.isna(df["OPQ_FULL_NAME"])) & (df["ORA_STATUS"] == "ACTIVE"),
                # Está Inactivo en RRHH, está en Opecus Desactivado y en Oracle Activo.
                # (pd.isna(df["COMMENTS"])) &
                (pd.isna(df["END_DATE"]) == False) & (len(str(df["END_DATE"])) > 7) &
                (df["NEW_END_DATE"] < datetime.now()) &
                (pd.isna(df["OPQ_FULL_NAME"]) == False) & (df["OPQ_STATUS"] == "Desactivado") &
                (df["ORA_STATUS"] == "ACTIVE"),
        ]
    result = ["Dar de baja Oracle (sólo Oracle)", "Dar de baja Oracle (sólo Opecus)", "Dar de baja Oracle (sólo RRHH)", "Dar de baja Oracle"]
    df["COMMENTS"] = np.select(cond, result, default=df["COMMENTS"])
    print("- Cargo los comentarios")

    # Ordeno la salida
    df.sort_values(['COMMENTS', 'EMAIL_ADDRESS'], ascending=[False, True], na_position='last', inplace=True)

    # self.prog.adv(80)

    # =========================================================================================
    # Filtro los candidatos a baja: las cuentas 'Disabled' en AD y las cuentas activas de EBS
    # =========================================================================================
    print("+ Filtro candidatos a baja")
    df2 = df[pd.isna(df["COMMENTS"]) == False]
    print(df2)
    print("- Filtro candidatos a baja")

    print(" "); print("df2 (Final): "); print(df2)

    # Limpio los nulls.
    df = df.fillna("")
    df2 = df2.fillna("")

    # Cambio el orden de las columnas (para mandar el df al QTableWidget de pantalla).
    # df = df[["ORA_USER_NAME", "ORA_STATUS", "ORA_END_DATE", "ORA_LAST_LOGON_DATE", "AD_STATUS", "AD_LAST_LOGON_DATE",
    #            "END_DATE", "ORA_FULL_NAME", "ORA_COUNTRY_CODE", "ORA_SUPERVISOR_NAME", "ORA_SUPER_SUPERVISOR",
    #            "EMAIL_ADDRESS", "ORA_EMPLOYEE_NUMBER", "AD_SAMACCOUNTNAME", "ORIGEN", "COMMENTS"]]
    df2 = df2[["ORA_USER_NAME", "ORA_STATUS", "ORA_END_DATE", "ORA_LAST_LOGON_DATE", "ORA_FULL_NAME",
               "FULL_NAME", "EMAIL_ADDRESS", "END_DATE", "OPQ_USERNAME", "OPQ_FULL_NAME", "OPQ_END_DATE",
               "AD_SAMACCOUNTNAME", "AD_STATUS", "AD_LAST_LOGON_DATE",
               "ORIGEN", "COMMENTS"]]
    # self.prog.adv(90)

    if self.completo:
        f.quick_excel(df, "Reporte_Oracle_Bajas (Full)", final=True, put_sftp=self.envia_sftp)
    else:
        f.quick_excel(df2, "Reporte_Oracle_Bajas", final=True, put_sftp=self.envia_sftp)

    print("Processing elapsed time {}".format(datetime.now() - start))
    print("- REPORTE NÓMINA VS ORACLE")
