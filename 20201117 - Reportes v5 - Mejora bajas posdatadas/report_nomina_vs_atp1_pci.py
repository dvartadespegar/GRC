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


def rep_nomina_vs_atp1pci(self, completo=False, envia_sftp=False, args=()):
    print(" "); print("+ REPORTE NOMINA VS ATP1-PCI")
    print("Params: completo =", completo, "; envia_sftp =", envia_sftp)
    start = datetime.now()

    df_nomina = args[0]

#
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
#         self.prog.setTitle("Reporte Nómina vs ATP1-PCI (Completo)" if self.completo else "Reporte Nómina vs ATP1-PCI")
#         self.prog.show()
#
#     def run(self):
#         start = datetime.datetime.now()
#         self.prog.adv(5)
#
#         # -----------------------------
#         # Nómina.
#         # -----------------------------
#         df = c.nomina()
#         self.prog.adv(40)

    # -----------------------------
    # ATP1.
    # -----------------------------
    atp1 = c.getATP1_PCI()
    df_atp1 = atp1.get_actives()
    # self.prog.adv(55)

    # ------------------------------------------------------------------------------------------------------
    # Hago el merge entre Nómina y GSuite.
    # ------------------------------------------------------------------------------------------------------
    # df = pd.merge(left=df, right=df_atp1, how='outer', left_on='EMAIL_ADDRESS', right_on='EMAIL_ADDRESS')
    df = pd.merge(left=df_nomina, right=df_atp1, how='right', left_on='EMAIL_ADDRESS', right_on='EMAIL_ADDRESS')

    # Cargo el Origen para aquellos registros que lo tienen vacío (porque son los únicos que no vienen de nómina, luego son de la app).
    df.loc[pd.isna(df["ORIGEN"]), "ORIGEN"] = "ATP1-PCI"

    print(" "); print("df (Merged):"); print(df)
    f.quick_excel(df, "Rep_GS_Merged")
    # self.prog.adv(65)

    # ------------------------------------------------------------------------------------------------------
    # Cargo la columna de comentarios (esta columna se crea al generar la nómina):
    # ------------------------------------------------------------------------------------------------------
    print("+ Cargo los comentarios")
    # for i in range(0, len(df.index)):
    #     if pd.isna(df.loc[i, "COMMENTS"]) and \
    #         pd.isna(df.loc[i, "END_DATE"]) == False and len(str(df.loc[i, "END_DATE"])) > 7 and \
    #         pd.isna(df.loc[i, "ATP1_STATUS"]) == False and df.loc[i, "ATP1_STATUS"] == "Activo": \
    #         df.loc[i, "COMMENTS"] = "Dar de Baja ATP1"
    #     if pd.isna(df.loc[i, "COMMENTS"]) and pd.isna(df.loc[i, "FULL_NAME"]) == True and pd.isna(df.loc[i, "OPQ_FULL_NAME"]) == True and \
    #         pd.isna(df.loc[i, "ATP1_STATUS"]) == False and df.loc[i, "ATP1_STATUS"] == "Activo":
    #         if pd.isna(df.loc[i, "AD_SAMACCOUNTNAME"]) == True:
    #             df.loc[i, "COMMENTS"] = "No relacionado a Nómina ni a AD - Sólo en ATP1"
    #         else:
    #             df.loc[i, "COMMENTS"] = "No relacionado a Nómina - Sólo en ATP1"
    # Convierto la fecha en campo date, además la copio en otro campo para evitar el error de slice.
    df["NEW_END_DATE"] = df.loc[:, "END_DATE"]
    df['NEW_END_DATE'] = pd.to_datetime(df['NEW_END_DATE'], format='%d/%m/%Y')

    df["COMMENTS"] = None
    cond =  [   # No está en RRHH ni en Opecus, y tiene ATP1 Activo (sólo ATP1)
                # (pd.isna(df["COMMENTS"])) &
                (pd.isna(df["FULL_NAME"])) & (pd.isna(df["OPQ_FULL_NAME"])) & (df["ATP1_STATUS"] == "Activo"),
                # No está en RRHH, está en Opecus Desactivado y en ATP1 Activo (sólo Opecus).
                # (pd.isna(df["COMMENTS"])) &
                (pd.isna(df["FULL_NAME"])) & (pd.isna(df["OPQ_FULL_NAME"]) == False) &
                (df["OPQ_STATUS"] == "Desactivado") & (df["ATP1_STATUS"] == "Activo"),
                # Está Inactivo en RRHH, no está en Opecus y tiene ATP1 Activo (sólo RRHH).
                # (pd.isna(df["COMMENTS"])) &
                (pd.isna(df["END_DATE"]) == False) & (len(str(df["END_DATE"])) > 7) &
                (df["NEW_END_DATE"] < datetime.now()) &
                (pd.isna(df["OPQ_FULL_NAME"])) & (df["ATP1_STATUS"] == "Activo"),
                # Está Inactivo en RRHH, está en Opecus Desactivado y en ATP1 Activo.
                # (pd.isna(df["COMMENTS"])) &
                (pd.isna(df["END_DATE"]) == False) & (len(str(df["END_DATE"])) > 7) &
                (df["NEW_END_DATE"] < datetime.now()) &
                (pd.isna(df["OPQ_FULL_NAME"]) == False) & (df["OPQ_STATUS"] == "Desactivado") &
                (df["ATP1_STATUS"] == "Activo"),
        ]
    result = ["Dar de baja ATP1-PCI (sólo ATP1-PCI)", "Dar de baja ATP1-PCI (sólo Opecus)", "Dar de baja ATP1-PCI (sólo RRHH)", "Dar de baja ATP1-PCI"]
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
               "ATP1_USERNAME", "ATP1_STATUS", "ATP1_LAST_LOGON_DATE", "ORIGEN", "COMMENTS"]]
    # df2 = df2[(pd.isna(df2["END_DATE"]) == False) & (pd.isna(df2["ATP1_STATUS"]) == False) & (df2["ATP1_STATUS"] == "Activo")]
    # self.prog.adv(90)

    print(" "); print("df2 (final):"); print(df2)

    # ----------------------------------------------------------------------------------------------------
    # Genero las planillas de salida: OJO! el nombre de la planilla no puede exceder los 30 caracteres.
    # ----------------------------------------------------------------------------------------------------
    if completo:
        f.quick_excel(df, "Reporte_Nomina_vs_ATP1_PCI (C)", final=True, put_sftp=self.envia_sftp)
    else:
        f.quick_excel(df2, "Reporte_Nomina_vs_ATP1_PCI", final=True, put_sftp=self.envia_sftp)

    print("Processing elapsed time {}".format(datetime.now() - start))  # esto muestra el tiempo de ejecución.
    print("- REPORTE NOMINA VS ATP1-PCI")
