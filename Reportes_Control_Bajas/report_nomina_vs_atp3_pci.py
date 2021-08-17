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


def rep_nomina_vs_atp3pci(self, completo=False, envia_sftp=False, args=()):
    print(" "); print("+ REPORTE NOMINA VS ATP3-PCI")
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
#         self.prog.setTitle("Reporte Nómina vs ATP3 (Completo)" if self.completo else "Reporte Nómina vs ATP3")
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

    # -----------------------------
    # ATP3.
    # -----------------------------
    atp3 = c.getATP3_PCI()
    df_atp3 = atp3.get_actives()
    # self.prog.adv(55)

    # ------------------------------------------------------------------------------------------------------
    # Hago el merge entre Nómina y GSuite.
    # ------------------------------------------------------------------------------------------------------
    df = pd.merge(left=df_nomina, right=df_atp3, how='right', left_on='EMAIL_ADDRESS', right_on='EMAIL_ADDRESS')

    # Cargo el Origen para aquellos registros que lo tienen vacío (porque son los únicos que no vienen de nómina, luego son de la app).
    df.loc[pd.isna(df["ORIGEN"]), "ORIGEN"] = "ATP3-PCI"

    print(" "); print("df (Merged):"); print(df)
    f.quick_excel(df, "Rep_ATP3_PCI_Merged")
    # self.prog.adv(65)

    # ----------------------------------------------
    # Calculo el AD_ELAPSED_DAYS
    # ----------------------------------------------
    print("+ Calculo el Elapsed_Days")
    df["ATP3_ELAPSED_DAYS"] = 0
    for i in range(0, len(df.index)):
        # print("email =", df.loc[i, "EMAIL_ADDRESS"], "; Start_Date =", df.loc[i, "ATP3_START_DATE"], "; Last_Login = ", df.loc[i, "ATP3_LAST_LOGON_DATE"])
        if pd.isna(df.loc[i, "ATP3_LAST_LOGON_DATE"]):
            if pd.isna(df.loc[i, "ATP3_START_DATE"]):
                df.loc[i, "ATP3_ELAPSED_DAYS"] = 0
            else:
                df.loc[i, "ATP3_ELAPSED_DAYS"] = abs(datetime.now() - datetime.strptime(df.loc[i, "ATP3_START_DATE"], "%d/%m/%Y %H:%M:%S")).days
        else:
            df.loc[i, "ATP3_ELAPSED_DAYS"] = abs(datetime.now() - datetime.strptime(df.loc[i, "ATP3_LAST_LOGON_DATE"], "%d/%m/%Y %H:%M:%S")).days
    print("- Calculo el Elapsed_Days")

    # ------------------------------------------------------------------------------------------------------
    # Cargo la columna de comentarios (esta columna se crea al generar la nómina):
    # ------------------------------------------------------------------------------------------------------
    print("+ Cargo los comentarios")
    # for i in range(0, len(df.index)):
    #     if pd.isna(df.loc[i, "COMMENTS"]) and \
    #         pd.isna(df.loc[i, "END_DATE"]) == False and len(str(df.loc[i, "END_DATE"])) > 7 and \
    #         pd.isna(df.loc[i, "ATP3_STATUS"]) == False and df.loc[i, "ATP3_STATUS"] == "Activo":
    #         df.loc[i, "COMMENTS"] = "Dar de Baja ATP3"
    #     if pd.isna(df.loc[i, "COMMENTS"]) and df.loc[i, "ATP3_SOURCE"] == "AD" and pd.isna(df.loc[i, "FULL_NAME"]) == True and \
    #         pd.isna(df.loc[i, "ATP3_STATUS"]) == False and df.loc[i, "ATP3_STATUS"] == "Activo":
    #         if pd.isna(df.loc[i, "AD_SAMACCOUNTNAME"]) == True:
    #             df.loc[i, "COMMENTS"] = "No relacionado a Nómina ni a AD - Sólo en ATP3"
    #         else:
    #             df.loc[i, "COMMENTS"] = "No relacionado a Nómina - Sólo en ATP3"
    #     if pd.isna(df.loc[i, "COMMENTS"]) and df.loc[i, "ATP3_SOURCE"] == "LOCAL" and df.loc[i, "ATP3_ELAPSED_DAYS"] > 90:
    #         df.loc[i, "COMMENTS"] = "Verificar Inactividad > 90 días"
    df["COMMENTS"] = None

    # Convierto la fecha en campo date, además la copio en otro campo para evitar el error de slice.
    df["NEW_END_DATE"] = df.loc[:, "END_DATE"]
    df['NEW_END_DATE'] = pd.to_datetime(df['NEW_END_DATE'], format='%d/%m/%Y')

    cond =  [   # ------------------------------------------------------------------------------------------------------
                # No está en RRHH ni en Opecus, y tiene ATP3 Activo (sólo ATP3) - No debe ser LOCAL
                # (pd.isna(df["COMMENTS"])) &
                (pd.isna(df["FULL_NAME"])) & (pd.isna(df["OPQ_FULL_NAME"])) & (df["ATP3_STATUS"] == "Activo") &
                (df["ATP3_SOURCE"] != "LOCAL"),
                # ------------------------------------------------------------------------------------------------------
                # No está en RRHH, está en Opecus Desactivado y en ATP3 Activo (sólo Opecus) - No debe ser LOCAL
                # (pd.isna(df["COMMENTS"])) &
                (pd.isna(df["FULL_NAME"])) & (pd.isna(df["OPQ_FULL_NAME"]) == False) &
                (df["OPQ_STATUS"] == "Desactivado") & (df["ATP3_STATUS"] == "Activo") & (df["ATP3_SOURCE"] != "LOCAL"),
                # ------------------------------------------------------------------------------------------------------
                # Está Inactivo en RRHH, no está en Opecus y tiene ATP3 Activo (sólo RRHH) - No debe ser LOCAL.
                # (pd.isna(df["COMMENTS"])) &
                (pd.isna(df["END_DATE"]) == False) & (len(str(df["END_DATE"])) > 7) &
                (df["NEW_END_DATE"] < datetime.now()) &
                (pd.isna(df["OPQ_FULL_NAME"])) & (df["ATP3_STATUS"] == "Activo") & (df["ATP3_SOURCE"] != "LOCAL"),
                # ------------------------------------------------------------------------------------------------------
                # Está Inactivo en RRHH, está en Opecus Desactivado y en ATP3 Activo - No debe ser LOCAL.
                # (pd.isna(df["COMMENTS"])) &
                (pd.isna(df["END_DATE"]) == False) & (len(str(df["END_DATE"])) > 7) &
                (df["NEW_END_DATE"] < datetime.now()) &
                (pd.isna(df["OPQ_FULL_NAME"]) == False) & (df["OPQ_STATUS"] == "Desactivado") &
                (df["ATP3_STATUS"] == "Activo") & (df["ATP3_SOURCE"] != "LOCAL"),
                # ------------------------------------------------------------------------------------------------------
                # Si no tiene otros comentarios, agrego el warning de inactividad > 90 días - Sólo para usuarios LOCALES.
                (pd.isna(df["COMMENTS"])) & (df["ATP3_ELAPSED_DAYS"] > 90) & (df["ATP3_SOURCE"] == "LOCAL"),
        ]
    result = ["Dar de baja ATP3-PCI (sólo ATP3)", "Dar de baja ATP3-PCI (sólo Opecus)", "Dar de baja ATP3-PCI (sólo RRHH)", "Dar de baja ATP3-PCI", "Verificar Inactividad > 90 días"]
    df["COMMENTS"] = np.select(cond, result, default=df["COMMENTS"])
    print("- Cargo los comentarios")

    # Ordeno la salida
    df.sort_values(['COMMENTS', 'EMAIL_ADDRESS'], ascending=[False, True], na_position='last', inplace=True)

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
    df2 = df2[["FULL_NAME", "EMAIL_ADDRESS", "END_DATE", "OPQ_USERNAME", "OPQ_FULL_NAME", "OPQ_END_DATE",
               "AD_SAMACCOUNTNAME", "AD_STATUS", "AD_LAST_LOGON_DATE",
               "ATP3_USERNAME", "ATP3_SOURCE", "ATP3_STATUS", "ATP3_START_DATE", "ATP3_LAST_LOGON_DATE", "ATP3_ELAPSED_DAYS", "ORIGEN", "COMMENTS"]]
    # self.prog.adv(90)

    print(" "); print("df2 (final):"); print(df2)

    # ------------------------------------------------------
    # Genero las planillas de salida.
    # ------------------------------------------------------
    if completo:
        f.quick_excel(df, "Reporte_Nomina_ATP3-PCI (Full)", final=True, put_sftp=self.envia_sftp)
    else:
        f.quick_excel(df2, "Reporte_Nomina_vs_ATP3-PCI", final=True, put_sftp=self.envia_sftp)

    print("Processing elapsed time {}".format(datetime.now() - start))  # esto muestra el tiempo de ejecución.
    print("- REPORTE NOMINA VS ATP3-PCI")