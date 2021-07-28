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


def rep_nomina_vs_vpn(self, completo=False, envia_sftp=False, args=()):
    print("+ REPORT NOMINA VS VPN")
    print("Params: completo =", completo, "; envia_sftp =", envia_sftp)
    start = datetime.now()

    df_vpn = args[0]
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
#         self.completo = args[0]         # chkCompleto.isChecked()
#         self.envia_sftp = args[1]       # envía al sftp
#         print("- __init__ de MiHilo")
#
#         print("+ Winbar")
#         self.prog = f.winbar()
#         self.prog.setTitle("Reporte Nómina vs VPN (Completo)" if self.completo else "Reporte Nómina vs VPN")
#         self.prog.show()
#         print("- Winbar")
#
#
#     def run(self):
#         start = datetime.datetime.now()
#         self.prog.adv(10)
#
#         # ----------------------------------------------------------------------------
#         # VPN.xlsx
#         # ----------------------------------------------------------------------------
#         df_vpn = c.getVPN()
#
#         print(" "); print("df_vpn (VPN):"); print(df_vpn)
#         print("- Busco VPN")
#
#         f.quick_excel(df_vpn, "Rep_VPN")
#         self.prog.adv(35)
#
#         # -----------------------------
#         # Nómina.
#         # -----------------------------
#         df = c.nomina()
#
#         # Necesito renombrar la columna del email, porque el join le cambia el nombre (porque es índice y le pone "index").
#         df.rename(columns={"index": "EMAIL_ADDRESS"}, inplace=True)

    # ------------------------------------------------------------------------------------------------------
    # Hago el merge con VPN.
    # ------------------------------------------------------------------------------------------------------
    # df = pd.merge(left=df, right=df_vpn, how='outer', left_on='EMAIL_ADDRESS', right_on='EMAIL_ADDRESS')
    df = pd.merge(left=df_nomina, right=df_vpn, how='right', left_on='EMAIL_ADDRESS', right_on='EMAIL_ADDRESS')

    # Cargo el Origen para aquellos registros que lo tienen vacío (porque son los únicos que no vienen de nómina, luego son de la app).
    df.loc[pd.isna(df["ORIGEN"]), "ORIGEN"] = "VPN"

    print(" "); print("df (Merged):"); print(df)
    f.quick_excel(df, "Rep_Merged")
    # self.prog.adv(70)

    # ------------------------------------------------------------------------------------------------------
    # Agrego la columna de comentarios:
    # ------------------------------------------------------------------------------------------------------
    print("+ Cargo los comentarios")

    # Carga el comentario de bajas: no tiene que tener comentarios ateriores, con fecha de baja, tiene que tener AD_STATUS y no debe estar en "Disabled".
    # for i in range(0, len(df.index)):
    #     if pd.isna(df.loc[i, "COMMENTS"]) and \
    #         pd.isna(df.loc[i, "END_DATE"]) == False and len(str(df.loc[i, "END_DATE"])) > 7 and \
    #         pd.isna(df.loc[i, "VPN_STATUS"]) == False and df.loc[i, "VPN_STATUS"] == True:
    #         df.loc[i, "COMMENTS"] = "Dar de Baja VPN"
    #     if pd.isna(df.loc[i, "COMMENTS"]) and pd.isna(df.loc[i, "FULL_NAME"]) == True and pd.isna(df.loc[i, "AD_SAMACCOUNTNAME"]) == True and \
    #         pd.isna(df.loc[i, "VPN_USER_NAME"]) == False:
    #         df.loc[i, "COMMENTS"] = "No asignado a Nómina ni AD - Sólo VPN"
    #     if pd.isna(df.loc[i, "COMMENTS"]) and pd.isna(df.loc[i, "FULL_NAME"]) == True and pd.isna(df.loc[i, "AD_SAMACCOUNTNAME"]) == False and \
    #         pd.isna(df.loc[i, "VPN_USER_NAME"]) == False:
    #         df.loc[i, "COMMENTS"] = "No asignado a Nómina - Sólo VPN"

    # Convierto la fecha en campo date, además la copio en otro campo para evitar el error de slice.
    df["NEW_END_DATE"] = df.loc[:, "END_DATE"]
    df['NEW_END_DATE'] = pd.to_datetime(df['NEW_END_DATE'], format='%d/%m/%Y')

    df["COMMENTS"] = None
    cond =  [   # No está en RRHH ni en Opecus, y tiene VPN Activo (sólo VPN)
                # (pd.isna(df["COMMENTS"])) &
                (pd.isna(df["FULL_NAME"])) & (pd.isna(df["OPQ_FULL_NAME"])) & (df["VPN_STATUS"]),

                # No está en RRHH, está en Opecus Desactivado y en VPN Activo (sólo Opecus).
                # (pd.isna(df["COMMENTS"])) &
                (pd.isna(df["FULL_NAME"])) & (pd.isna(df["OPQ_FULL_NAME"]) == False) &
                (df["OPQ_STATUS"] == "Desactivado") & (df["VPN_STATUS"]),

                # Está Inactivo en RRHH, no está en Opecus y tiene VPN Activo (sólo RRHH).
                # (pd.isna(df["COMMENTS"])) &
                (pd.isna(df["END_DATE"]) == False) & (len(str(df["END_DATE"])) > 7) &
                (df["NEW_END_DATE"] < datetime.now()) &
                (pd.isna(df["OPQ_FULL_NAME"])) & (df["VPN_STATUS"]),

                # Está Inactivo en RRHH, está en Opecus Desactivado y en VPN Activo.
                # (pd.isna(df["COMMENTS"])) &
                (pd.isna(df["END_DATE"]) == False) & (len(str(df["END_DATE"])) > 7) &
                (df["NEW_END_DATE"] < datetime.now()) &
                (pd.isna(df["OPQ_FULL_NAME"]) == False) & (df["OPQ_STATUS"] == "Desactivado") &
                (df["VPN_STATUS"]),
        ]
    result = ["Dar de baja VPN (sólo VPN)", "Dar de baja VPN (sólo Opecus)", "Dar de baja VPN (sólo RRHH)", "Dar de baja VPN"]
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
    # df = df[["FULL_NAME", "EMAIL_ADDRESS", "END_DATE", "AD_STATUS", "VPN_LAST_LOGON_DATE", "VPN_STATUS",
    #          "VPN_TOKEN_ENABLED", "AD_SAMACCOUNTNAME", "VPN_USER_NAME", "COUNTRY_CODE", "SUPERVISOR_NAME", "ORIGEN", "COMMENTS"]]
    df2 = df2[["FULL_NAME", "EMAIL_ADDRESS", "END_DATE", "OPQ_USERNAME", "OPQ_FULL_NAME", "OPQ_END_DATE",
               "AD_SAMACCOUNTNAME", "AD_STATUS", "AD_LAST_LOGON_DATE",
               "VPN_USER_NAME", "VPN_LAST_LOGON_DATE", "VPN_STATUS", "ORIGEN", "COMMENTS"]]

    if self.completo:
        f.quick_excel(df, "Reporte_Nomina_vs_VPN (Full)", final=True, put_sftp=self.envia_sftp)
    else:
        f.quick_excel(df2, "Reporte_Nomina_vs_VPN", final=True, put_sftp=self.envia_sftp)

    print("Processing elapsed time {}".format(datetime.now() - start))
    print("- REPORT NOMINA VS VPN")
