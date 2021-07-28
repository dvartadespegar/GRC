import pandas as pd
from datetime import datetime
import threading

import funcs as f
import connections as c


#pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


def reportInactivity(self, inactive_days=60, completo=False, envia_sftp=False, args=()):
    print("+ INICIO REPORT INACTIVITY")
    print("  completo =", completo, "; envia_sftp =", envia_sftp)
    print("  args =", args)
    start = datetime.now()

    df_ora = args[0]
    df_nomina = args[1]
    df_vpn = args[2]

#     # ========================================================================
#     # THREAD para el procesamiento del df (para independizar el progress_bar)
#     # ========================================================================
#     print("+ HiloInact")
#     hilo = HiloInact(args=(inactive_days, completo, envia_sftp, ), daemon=False)
#     print("- HiloInact")
#     hilo.start()
#
#     print("isAlive? (después del while) = ", hilo.is_alive())
#     print("/======= FIN REPORT INACTIVITY =======/")
#
#
# class HiloInact(threading.Thread):
#     def __init__(self, group=None, target=None, name=None, args=(), kwargs=None, *, daemon=None):
#         print("+ __init__ de MiHilo")
#         super().__init__(group=group, target=target, name=name, daemon=daemon)
#         print(args)
#         self.days = args[0]     # Cant días
#         self.completo = args[1]  # chkCompleto.isChecked()
#         self.envia_sftp = args[2]   # envía al sftp
#         print("- __init__ de MiHilo")
#
#         self.prog = f.winbar()
#         self.prog.setTitle("Oracle - Reporte de Inactividad (Completo)" if self.completo else "Oracle - Reporte de Inactividad")
#         self.prog.show()
#
#     def run(self):
#         dt_start = datetime.now()
#     self.prog.adv(5)

    # ----------------------------------------------
    # Traigo el df de Oracle.
    # ----------------------------------------------
    # oracle = c.getOracle()
    # df = oracle.get_actives()
    # self.prog.adv(25)

    # ----------------------------------------------
    # Traigo la nómina (RRHH + Opecus + AD)
    # ----------------------------------------------
    # df_nomina = c.nomina()
    # self.prog.adv(40)

    df_nomina["AD_ELAPSED_DAYS"] = 0

    # ----------------------------------------------
    print("+ Merge con Nómina")
    # ----------------------------------------------
    df = pd.merge(left=df_ora, right=df_nomina, how='left', left_on='EMAIL_ADDRESS', right_on='EMAIL_ADDRESS')
    print("- Merge con Nómina")
    print(" "); print("df (Merged con Nómina): "); print(df) #; print(df.info())
    f.quick_excel(df, "Rep_Oracle_Inact(Merge Nómina)")
    # self.prog.adv(50)

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
    # self.prog.adv(70)

    # De los registros que vinieron con status = "DISABLE" en el AD, voy a buscar las cuentas de Opecus, para ser más explícito en la salida del reporte.
    print("+ Rellena valores de Opecus")
    opq = c.getOpecus()
    for i in range(0, len(df.index)):
        if pd.isna(df.loc[i, "AD_STATUS"]) == False:
            if df.loc[i, "AD_STATUS"].lower().find("disable") >= 0:
                arr = opq.get_user(df.loc[i, "EMAIL_ADDRESS"])
                if pd.isna(arr[6]) == False and pd.isna(df.loc[i, "END_DATE"]) == True:
                    df.loc[i, "END_DATE"] = arr[6]
    opq.close()
    print("- Rellena valores de Opecus")
    # self.prog.adv(80)


    # ----------------------------------------------
    print("+ Merge con VPN")
    # ----------------------------------------------
    # df_vpn = c.getVPN()
    df = pd.merge(left=df, right=df_vpn, how='left', left_on='EMAIL_ADDRESS', right_on='EMAIL_ADDRESS')
    print("- Merge con VPN")
    print(" "); print("df (Merged con VPN): "); print(df) #; print(df.info())
    f.quick_excel(df, "Rep_Oracle_Inact (Merge VPN)")


    # ----------------------------------------------
    # Calculo el AD_ELAPSED_DAYS
    # ----------------------------------------------
    for i in range(0, len(df.index)):
        if pd.isna(df.loc[i, "AD_LAST_LOGON_DATE"]):
            if pd.isna(df.loc[i, "START_DATE"]):
                df.loc[i, "AD_ELAPSED_DAYS"] = 0
            else:
                df.loc[i, "AD_ELAPSED_DAYS"] = abs(datetime.now() - datetime.strptime(df.loc[i, "START_DATE"], "%d/%m/%Y")).days
        else:
            df.loc[i, "AD_ELAPSED_DAYS"] = abs(datetime.now() - datetime.strptime(df.loc[i, "AD_LAST_LOGON_DATE"], "%d/%m/%Y %H:%M")).days


    # ----------------------------------------------
    # Calculo el VPN_ELAPSED_DAYS
    # ----------------------------------------------
    df["VPN_ELAPSED_DAYS"] = -1
    for i in range(0, len(df.index)):
        if pd.isna(df.loc[i, "VPN_LAST_LOGON_DATE"]):
            if pd.isna(df.loc[i, "START_DATE"]):        # Este dato no existe en la VPN, así que tomo el del AD.
                df.loc[i, "VPN_ELAPSED_DAYS"] = 0
            else:
                df.loc[i, "VPN_ELAPSED_DAYS"] = abs(datetime.now() - datetime.strptime(df.loc[i, "START_DATE"], "%d/%m/%Y")).days
        else:
            df.loc[i, "VPN_ELAPSED_DAYS"] = abs(datetime.now() - datetime.strptime(df.loc[i, "VPN_LAST_LOGON_DATE"], "%d/%m/%Y %H:%M")).days



    # self.prog.adv(60)
    print(" "); print("df (con elapsed_days): "); print(df) #; print(df.info())
    f.quick_excel(df, "Rep_Oracle_Inact (completo)")

    df["COMMENTS"] = None

    for i in range(0, len(df.index)):
        if pd.isna(df.loc[i, "COMMENTS"]):
            if pd.isna(df.loc[i, "END_DATE"]) == False and len(str(df.loc[i, "END_DATE"])) > 3 and \
                datetime.strptime(df.loc[i, "END_DATE"], "%d/%m/%Y") < datetime.now() and df.loc[i, "ORA_STATUS"] == "ACTIVE" and \
                pd.isna(df.loc[i, "ORA_END_DATE"]):
                df.loc[i, "COMMENTS"] = "Dar de Baja Oracle EBS"
            elif pd.isna(df.loc[i, "END_DATE"]) == False and len(str(df.loc[i, "END_DATE"])) > 3 and \
                datetime.strptime(df.loc[i, "END_DATE"], "%d/%m/%Y") < datetime.now() and df.loc[i, "ORA_STATUS"] == "ACTIVE" and \
                (not pd.isna(df.loc[i, "ORA_END_DATE"]) and (datetime.strptime(df.loc[i, "ORA_END_DATE"], "%d/%m/%Y") > datetime.now())):
                df.loc[i, "COMMENTS"] = "De baja - En uso por Ctas a Pagar"
            elif df.loc[i, "ORA_ELAPSED_DAYS"] >= 90 and df.loc[i, "AD_ELAPSED_DAYS"] >= 90 and df.loc[i, "VPN_ELAPSED_DAYS"] >= 90:
                    df.loc[i, "COMMENTS"] = "Candidato a Baja"
            elif df.loc[i, "ORA_ELAPSED_DAYS"] >= 60 and df.loc[i, "AD_ELAPSED_DAYS"] >= 60 and df.loc[i, "VPN_ELAPSED_DAYS"] >= 60:
                    df.loc[i, "COMMENTS"] = "Dar 2° Aviso"
            elif df.loc[i, "ORA_ELAPSED_DAYS"] >= 30 and df.loc[i, "AD_ELAPSED_DAYS"] >= 30 and df.loc[i, "VPN_ELAPSED_DAYS"] >= 30:
                    df.loc[i, "COMMENTS"] = "Dar 1er Aviso"

    print(" "); print("df (con comentarios): "); print(df);
    # self.prog.adv(90)

    # ----------------------------------------------
    # Filtro la columna de comentarios con valor.
    # ----------------------------------------------
    print("+ Filtro elapsed_days > 60")
    df2 = df[pd.isna(df["COMMENTS"]) == False]
    print("- Filtro elapsed_days > 60")
    print("df2 (filtrado): "); print(df2)
    # self.prog.adv(95)

    # ----------------------------------------------
    # Dejo sólo las columnas que me interesan.
    # ----------------------------------------------
    # df = df[["ORA_USER_NAME", "ORA_STATUS", "ORA_END_DATE", "END_DATE", "ORA_LAST_LOGON_DATE", "ORA_ELAPSED_DAYS",
    #          "START_DATE", "AD_LAST_LOGON_DATE", "AD_ELAPSED_DAYS", "AD_STATUS", "EMAIL_ADDRESS",
    #          "AD_SAMACCOUNTNAME", "FULL_NAME", "ORA_COUNTRY_CODE", "ORA_SUPERVISOR_NAME", "OPQ_USERNAME", "COMMENTS"]]
    df2 = df2[["ORA_USER_NAME", "ORA_STATUS", "ORA_END_DATE", "START_DATE", "END_DATE",
               "ORA_LAST_LOGON_DATE", "AD_LAST_LOGON_DATE", "VPN_LAST_LOGON_DATE",
               "ORA_ELAPSED_DAYS", "AD_ELAPSED_DAYS", "VPN_ELAPSED_DAYS",
               "AD_STATUS", "VPN_STATUS", "EMAIL_ADDRESS",
               "FULL_NAME", "AD_SAMACCOUNTNAME", "OPQ_USERNAME", "VPN_USER_NAME", "COMMENTS"]]

    if self.completo:
        f.quick_excel(df, "Reporte_Oracle_Inactiv (Full)", final=True, put_sftp=self.envia_sftp)
    else:
        f.quick_excel(df2, "Reporte_Oracle_Inactividad", final=True, put_sftp=self.envia_sftp)

    print("Processing elapsed time {}".format(datetime.now() - start))
    print("- INICIO REPORT INACTIVITY")
