import pandas as pd
import numpy as np
import threading
import funcs as f
from datetime import datetime
import connections as c


#pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


def rep_nomina_vs_sox(self, completo=False, envia_sftp=False, args=()):
    print(" "); print("+ REPORTE NOMINA VS APPS SOX")
    print("Params: completo =", completo, "; envia_sftp =", envia_sftp)
    start = datetime.now()

    df_nomina = args[0]

#     print("+ MiHilo")
#     hilo = MiHilo(args=(completo, envia_sftp, ), daemon=False)
#     print("- MiHilo")
#     hilo.start()
#     print("isAlive? (después del while) = ", hilo.is_alive())
#
# class MiHilo(threading.Thread):
#     def __init__(self, group=None, target=None, name=None, args=(), kwargs=None, *, daemon=None):
#         print("+ __init__ de MiHilo")
#         super().__init__(group=group, target=target, name=name, daemon=daemon)
#         print(args)
#         self.completo = args[0]
#         self.envia_sftp = args[1]   # envía al sftp
#         print("- __init__ de MiHilo")
#
#         self.prog = f.winbar()
#         self.prog.setTitle("Reporte Nómina vs SOX (Completo)" if self.completo else "Reporte Nómina vs SOX")
#         self.prog.show()
#
#     def run(self):
#         start = datetime.now()
#         self.prog.adv(5)
#
#         # -----------------------------
#         # Nómina.
#         # -----------------------------
#         df = c.nomina()
#         self.prog.adv(40)

    # --------------------------------------------------------------
    # Lee la planilla "Reportes aplicaciones SOX.xlsx" (en RRHH)
    # --------------------------------------------------------------
    df_sox = c.get_SOX()
    # self.prog.adv(55)

    # ------------------------------------------------------------------------------------------------------
    # Hago el merge entre Nómina y GSuite.
    # ------------------------------------------------------------------------------------------------------
    df = pd.merge(left=df_nomina, right=df_sox, how='right', left_on='EMAIL_ADDRESS', right_on='EMAIL_ADDRESS')

    # Cargo el Origen para aquellos registros que lo tienen vacío (porque son los únicos que no vienen de nómina, luego son de la app).
    df.loc[pd.isna(df["ORIGEN"]), "ORIGEN"] = "SOX"

    # Ordeno el df por Aplicación / Email.
    df = df.sort_values(['SOX_APPS', 'EMAIL_ADDRESS'], ascending=[True, True])

    print(" "); print("df (Merged):"); print(df)
    f.quick_excel(df, "Rep_SOX_Merged")
    # self.prog.adv(65)

    """
    # ----------------------------------------------
    # Calculo el AD_ELAPSED_DAYS
    # ----------------------------------------------
    print("+ Calculo el Elapsed_Days")
    df["SOX_ELAPSED_DAYS"] = 0
    for i in range(0, len(df.index)):
        # print("email =", df.loc[i, "EMAIL_ADDRESS"], "; Start_Date =", df.loc[i, "ATP3_START_DATE"], "; Last_Login = ", df.loc[i, "ATP3_LAST_LOGON_DATE"])
        if pd.isna(df.loc[i, "SOX_LAST_LOGON_DATE"]):
            if pd.isna(df.loc[i, "SOX_START_DATE"]):
                df.loc[i, "SOX_ELAPSED_DAYS"] = 0
            else:
                df.loc[i, "SOX_ELAPSED_DAYS"] = abs(datetime.now() - datetime.strptime(df.loc[i, "SOX_START_DATE"], "%d/%m/%Y %H:%M:%S")).days
        else:
            df.loc[i, "SOX_ELAPSED_DAYS"] = abs(datetime.now() - datetime.strptime(df.loc[i, "SOX_LAST_LOGON_DATE"], "%d/%m/%Y %H:%M:%S")).days
    print("- Calculo el Elapsed_Days")
    """

    # ------------------------------------------------------------------------------------------------------
    # Cargo la columna de comentarios (esta columna se crea al generar la nómina):
    # ------------------------------------------------------------------------------------------------------
    print("+ Cargo los comentarios")
    # for i in range(0, len(df.index)):
    #     if pd.isna(df.loc[i, "COMMENTS"]) and \
    #         pd.isna(df.loc[i, "END_DATE"]) == False and len(str(df.loc[i, "END_DATE"])) > 7:
    #         df.loc[i, "COMMENTS"] = "Dar de Baja"
    #     if pd.isna(df.loc[i, "COMMENTS"]) and pd.isna(df.loc[i, "FULL_NAME"]) == True and pd.isna(df.loc[i, "OPQ_FULL_NAME"]) == True:
    #         df.loc[i, "COMMENTS"] = "No relacionado a Nómina"
    #     if pd.isna(df.loc[i,"COMMENTS"]) and pd.isna(df.loc[i,"AD_SAMACCOUNTNAME"]) == True:
    #         df.loc[i, "COMMENTS"] = "No relacionado al AD"

    # Convierto la fecha en campo date, además la copio en otro campo para evitar el error de slice.
    df["NEW_END_DATE"] = df.loc[:, "END_DATE"]
    df['NEW_END_DATE'] = pd.to_datetime(df['NEW_END_DATE'], format='%d/%m/%Y')

    df["COMMENTS"] = None
    cond =  [   # No está en RRHH ni en Opecus, y tiene SOX Activo (sólo SOX)
                # (pd.isna(df["COMMENTS"])) &
                (pd.isna(df["FULL_NAME"])) & (pd.isna(df["OPQ_FULL_NAME"])) & (pd.isna(df["SOX_USER_NAME"]) == False),

                # No está en RRHH, está en Opecus Desactivado y en SOX Activo (sólo Opecus).
                # (pd.isna(df["COMMENTS"])) &
                (pd.isna(df["FULL_NAME"])) & (pd.isna(df["OPQ_FULL_NAME"]) == False) &
                (df["OPQ_STATUS"] == "Desactivado") & (pd.isna(df["SOX_USER_NAME"]) == False),

                # Está Inactivo en RRHH, no está en Opecus y tiene SOX Activo (sólo RRHH).
                # (pd.isna(df["COMMENTS"])) &
                (pd.isna(df["END_DATE"]) == False) & (len(str(df["END_DATE"])) > 7) &
                (df["NEW_END_DATE"] < datetime.now()) &
                (pd.isna(df["OPQ_FULL_NAME"])) & (pd.isna(df["SOX_USER_NAME"]) == False),

                # Está Inactivo en RRHH, está en Opecus Desactivado y en SOX Activo.
                # (pd.isna(df["COMMENTS"])) &
                (pd.isna(df["END_DATE"]) == False) & (len(str(df["END_DATE"])) > 7) &
                (df["NEW_END_DATE"] < datetime.now()) &
                (pd.isna(df["OPQ_FULL_NAME"]) == False) & (df["OPQ_STATUS"] == "Desactivado") &
                (pd.isna(df["SOX_USER_NAME"]) == False),
        ]
    result = ["Dar de baja SOX (sólo SOX)", "Dar de baja SOX (sólo Opecus)", "Dar de baja SOX (sólo RRHH)", "Dar de baja SOX"]
    df["COMMENTS"] = np.select(cond, result, default=df["COMMENTS"])
    print("- Cargo los comentarios")

    # Ordeno la salida
    df.sort_values(['SOX_APPS', 'COMMENTS', 'EMAIL_ADDRESS'], ascending=[True, False, True], na_position='last', inplace=True)

    print(" "); print("df (Merged con comentarios):"); print(df)
    # self.prog.adv(70)

    # ------------------------------------------------------
    # Dejo sólo las filas con Comentarios.
    # ------------------------------------------------------
    print("+ Filtro bajas")
    df2 = df[pd.isna(df["COMMENTS"]) == False]
    print("- Filtro bajas")
    # self.prog.adv(80)

    # ----------------------------------------------------------------------------------------------------------------
    # Dejo sólo las columnas que me interesan, y las filas que tienen end_date y están Activas en ATP1.
    # ----------------------------------------------------------------------------------------------------------------
    # df = df[["SOX_APPS", "EMAIL_ADDRESS", "FULL_NAME", "END_DATE", "OPQ_END_DATE", "COUNTRY_CODE", "START_DATE",
    #          "SUPERVISOR_NAME", "OPQ_USERNAME", "OPQ_CONSULTORA", "PCI", "AD_SAMACCOUNTNAME", "AD_COUNTRY_CODE", "AD_STATUS",
    #          "AD_LAST_LOGON_DATE", "SOX_USER_NAME", "SOX_FULL_NAME", "SOX_START_DATE", "SOX_LAST_LOGON_DATE", "ORIGEN", "COMMENTS"]]
    df2 = df2[["SOX_APPS", "EMAIL_ADDRESS", "FULL_NAME", "END_DATE", "OPQ_USERNAME", "OPQ_FULL_NAME", "OPQ_END_DATE",
               "AD_SAMACCOUNTNAME", "AD_STATUS", "AD_LAST_LOGON_DATE",
               "SOX_USER_NAME", "SOX_START_DATE", "SOX_LAST_LOGON_DATE", "COMMENTS"]]
    # self.prog.adv(90)

    print(" "); print("df2 (final):"); print(df2)

    # ------------------------------------------------------
    # Genero las planillas de salida.
    # ------------------------------------------------------
    if self.completo:
        f.quick_excel(df, "Reporte_Nomina_vs_SOX (Full)", final=True, put_sftp=self.envia_sftp)
    else:
        f.quick_excel(df2, "Reporte_Nomina_vs_SOX", final=True, put_sftp=self.envia_sftp)

    print("Processing elapsed time {}".format(datetime.now() - start))  # esto muestra el tiempo de ejecución.
    print("- REPORTE NOMINA VS APPS SOX")
