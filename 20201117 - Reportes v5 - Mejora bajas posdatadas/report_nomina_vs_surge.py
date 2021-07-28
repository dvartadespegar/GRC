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


def rep_nomina_vs_surgemail(self, completo=False, envia_sftp=False, args=()):
    print(" "); print("+ REPORT NOMINA VS SURGE")
    print("Params: completo =", completo, "; envia_sftp =", envia_sftp)
    start = datetime.now()

    df_surge = args[0]
    df_nomina = args[1]


#     # ========================================================================
#     # THREAD para el procesamiento del df (para independizar el progress_bar)
#     # ========================================================================
#     print("+ MiHilo")
#     hilo = MiHilo(args=(completo, envia_sftp, ), daemon=False)
#     print("- MiHilo")
#     hilo.start()
#
#     print("isAlive? (después del while) = ", hilo.is_alive())
#     print("/======= FIN REPORT AD vs ORACLE =======/")
#
#
# class MiHilo(threading.Thread):
#     def __init__(self, group=None, target=None, name=None, args=(), kwargs=None, *, daemon=None):
#         print("+ __init__ de MiHilo")
#         super().__init__(group=group, target=target, name=name, daemon=daemon)
#         print(args)
#         self.completo = args[0]     # chkCompleto.isChecked()
#         self.envia_sftp = args[1]   # envía al sftp
#         print("- __init__ de MiHilo")
#
#         print("+ Winbar")
#         self.prog = f.winbar()
#         self.prog.setTitle("Reporte Nómina vs SurgeMail (Completo)" if self.completo else "Reporte Nómina vs SurgeMail")
#         self.prog.show()
#         print("- Winbar")
#
#
#     def run(self):
#         start = datetime.datetime.now()
#         self.prog.adv(10)
#
#         # ----------------------------------------------------------------------------
#         # Trae el archivo de SurgeMail (nwauth.txt) del SFTP.
#         # ----------------------------------------------------------------------------
#         # f.getSftp(remote_path="/upload/surge/", filename="nwauth.txt")
#
#         # ----------------------------------------------------------------------------
#         # SurgeMail (nwauth.txt)
#         # ----------------------------------------------------------------------------
#         df_sm = c.getSurgeMail()
#
#         print(" "); print("df_sm (SurgeMail):"); print(df_sm)
#         f.quick_excel(df_sm, "Rep_SurgeMail")
#         self.prog.adv(35)
#
#         # -----------------------------
#         # Nómina.
#         # -----------------------------
#         df = c.nomina()

    # ------------------------------------------------------------------------------------------------------
    # Hago el merge.
    # ------------------------------------------------------------------------------------------------------
    # df = pd.merge(left=df, right=df_sm, how='inner', left_on='EMAIL_ADDRESS', right_on='EMAIL_ADDRESS')  # dejo sólo los registros que matchean en ambos lados.
    df = pd.merge(left=df_nomina, right=df_surge, how='right', left_on='EMAIL_ADDRESS', right_on='EMAIL_ADDRESS')

    # Cargo el Origen para aquellos registros que lo tienen vacío (porque son los únicos que no vienen de nómina, luego son de la app).
    df.loc[pd.isna(df["ORIGEN"]), "ORIGEN"] = "SURGEMAIL"

    print(" "); print("df (Merged):"); print(df)
    f.quick_excel(df, "Rep_Merged")
    # self.prog.adv(70)

    # ------------------------------------------------------------------------------------------------------
    # Agrego la columna de comentarios:
    # ------------------------------------------------------------------------------------------------------
    print("+ Cargo los comentarios")
    # for i in range(0, len(df.index)):
    #     if pd.isna(df.loc[i, "COMMENTS"]) and \
    #         pd.isna(df.loc[i, "END_DATE"]) == False and len(str(df.loc[i, "END_DATE"])) > 7 and \
    #         pd.isna(df.loc[i, "EMAIL_ADDRESS"]) == False and \
    #         df.loc[i, "SM_STATUS"] not in ("cancelled","closed","suspended"):
    #         df.loc[i, "COMMENTS"] = "Dar de Baja SurgeMail"
    #     if pd.isna(df.loc[i,"COMMENTS"]) and pd.isna(df.loc[i,"FULL_NAME"]) == True and pd.isna(df.loc[i,"OPQ_FULL_NAME"]) and \
    #         (pd.isna(df.loc[i,"SM_STATUS"]) == True or (pd.isna(df.loc[i,"SM_STATUS"]) == False and df.loc[i, "SM_STATUS"] == "ok")):
    #         df.loc[i, "COMMENTS"] = "No relacionado a Nómina - Sólo en SurgeMail"

    # Convierto la fecha en campo date, además la copio en otro campo para evitar el error de slice.
    df["NEW_END_DATE"] = df.loc[:, "END_DATE"]
    df['NEW_END_DATE'] = pd.to_datetime(df['NEW_END_DATE'], format='%d/%m/%Y')

    df["COMMENTS"] = None
    cond =  [   # No está en RRHH ni en Opecus, y tiene Surge Activo (sólo Surge)
                # (pd.isna(df["COMMENTS"])) &
                (pd.isna(df["FULL_NAME"])) & (pd.isna(df["OPQ_FULL_NAME"])) & (pd.isna(df["SM_CREATION_DATE"]) == False),

                # No está en RRHH, está en Opecus Desactivado y en Surge Activo (sólo Opecus).
                # (pd.isna(df["COMMENTS"])) &
                (pd.isna(df["FULL_NAME"])) & (pd.isna(df["OPQ_FULL_NAME"]) == False) &
                (df["OPQ_STATUS"] == "Desactivado") & (pd.isna(df["SM_CREATION_DATE"]) == False),

                # Está Inactivo en RRHH, no está en Opecus y tiene Surge Activo (sólo RRHH).
                # (pd.isna(df["COMMENTS"])) &
                (pd.isna(df["END_DATE"]) == False) & (len(str(df["END_DATE"])) > 7) &
                (df["NEW_END_DATE"] < datetime.now()) &
                (pd.isna(df["OPQ_FULL_NAME"])) & (pd.isna(df["SM_CREATION_DATE"]) == False),

                # Está Inactivo en RRHH, está en Opecus Desactivado y en Surge Activo.
                # (pd.isna(df["COMMENTS"])) &
                (pd.isna(df["END_DATE"]) == False) & (len(str(df["END_DATE"])) > 7) &
                (df["NEW_END_DATE"] < datetime.now()) &
                (pd.isna(df["OPQ_FULL_NAME"]) == False) & (df["OPQ_STATUS"] == "Desactivado") &
                (pd.isna(df["SM_CREATION_DATE"]) == False),
        ]
    result = ["Dar de baja SurgeMail (sólo SurgeMail)", "Dar de baja SurgeMail (sólo Opecus)", "Dar de baja SurgeMail (sólo RRHH)", "Dar de baja SurgeMail"]
    df["COMMENTS"] = np.select(cond, result, default=df["COMMENTS"])
    print("- Cargo los comentarios")

    # Ordeno la salida
    df.sort_values(['COMMENTS', 'EMAIL_ADDRESS'], ascending=[False, True], na_position='last', inplace=True)

    print(" "); print("df (Merged con comentarios):"); print(df)

    # ------------------------------------------------------
    # Dejo sólo las filas con Comentarios
    # ------------------------------------------------------
    print("+ Filtro bajas")
    df2 = df[pd.isna(df["COMMENTS"]) == False]
    print("- Filtro bajas")
    # self.prog.adv(80)

    # Limpio los valores nulos (ojo! después de esto, ya no puedo preguntar más por is null).
    print("+ Limpio los df")
    df = df.fillna("", inplace=False)
    df2 = df2.fillna("", inplace=False)
    print("- Limpio los df")
    # self.prog.adv(90)

    # ----------------------------------------------------------------------------------------------------------------
    # Dejo sólo las columnas que me interesan y les cambio el orden (para mandar el df al QTableWidget de pantalla).
    # ----------------------------------------------------------------------------------------------------------------
    print("+ Dejo sólo las columnas que me interesan")
    # df = df[["FULL_NAME", "EMAIL_ADDRESS", "END_DATE", "AD_STATUS", "AD_LAST_LOGON_DATE", "SM_FULL_NAME",
    #          "SM_STATUS", "AD_SAMACCOUNTNAME", "COUNTRY_CODE", "SUPERVISOR_NAME", "ORIGEN", "COMMENTS"]]
    df2 = df2[["FULL_NAME", "EMAIL_ADDRESS", "END_DATE", "OPQ_USERNAME", "OPQ_FULL_NAME", "OPQ_END_DATE",
               "AD_SAMACCOUNTNAME", "AD_STATUS", "AD_LAST_LOGON_DATE",
               "SM_FULL_NAME", "SM_STATUS", "SM_LAST_LOGON_DATE", "ORIGEN", "COMMENTS"]]
    print("- Dejo sólo las columnas que me interesan")

    # OJO! los nombres de las planillas Excel deben tener hasta 30 caracteres!
    if self.completo:
        f.quick_excel(df, "Reporte_Nomina_vs_Surge (Full)", final=True, put_sftp=self.envia_sftp)
    else:
        f.quick_excel(df2, "Reporte_Nomina_vs_SurgeMail", final=True, put_sftp=self.envia_sftp)

    print("Processing elapsed time {}".format(datetime.now() - start))
    print("- REPORT NOMINA VS SURGE")
