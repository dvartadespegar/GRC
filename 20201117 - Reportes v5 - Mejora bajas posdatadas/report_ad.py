import pandas as pd
import numpy as np
import threading
from datetime import datetime
import time
import funcs as f
import connections as c

#pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


def reportADInactivity(self, completo=False, envia_sftp=False, args=()):
    print(" "); print("+ REPORTE AD INACTIVITY")
    print("Params: completo: ", completo, "; envia_sftp =", envia_sftp)
    start = datetime.now()

    df_vpn = args[0]
    df_gsuite = args[1]
    df_surge = args[2]

#
#     print("+ MiHilo")
#     hilo = MiHilo(args=("Activos", "A", envia_sftp, ), daemon=False)
#     hilo.start()
#     print("- MiHilo")
#
# def reportADCompleto(self, envia_sftp=False):
#     print("+ MiHilo")
#     hilo = MiHilo(args=("Completo", "C", envia_sftp, ), daemon=False)
#     print("- MiHilo")
#     hilo.start()
#
# def reportADComputers(self, envia_sftp=False):
#     print("+ MiHilo")
#     hilo = MiHilo(args=("Computers", "P", envia_sftp, ), daemon=False)
#     print("- MiHilo")
#     hilo.start()
#
#
# class MiHilo(threading.Thread):
#     def __init__(self, group=None, target=None, name=None, args=(), kwargs=None, *, daemon=None):
#         print("+ __init__ de MiHilo")
#         super().__init__(group=group, target=target, name=name, daemon=daemon)
#         print(args)
#         self.reportType = args[0]   #ipo de reporte: (A)ctivo / (C)ompleto / (P)Computers
#         self.rtype = args[1]
#         self.envia_sftp = args[2]   # envía al sftp
#         print("self.reporType = ", self.reportType)
#         print("- __init__ de MiHilo")
#
#         self.w = f.wemerge()
#         self.w.show()
#
#
#     def run(self):
#         print("+ RUN")
#         dt_start = datetime.now()
#         print("run: reportType = ", self.reportType)
#         print("run: rtype = ", self.rtype)

    # ----------------------------------------------------------------------------
    # AD
    # ----------------------------------------------------------------------------
    ad = c.getAD()

    if completo:
        df_ad = ad.get_actives()
        df_inactivos = ad.get_inactives()
        # Concateno los activos con los inactivos.
        df_ad = pd.concat([df_ad, df_inactivos])
        filename = "Reporte_AD_Inactividad_(Full)"
    else:
        df_ad = ad.get_actives()
        filename = "Reporte_AD_Inactividad"

    # ------------------------------------------------------------
    # Agrego VPN y GSuite para traer el Last_Logon más reciente
    # ------------------------------------------------------------
    # df_vpn = c.getVPN()
    df_vpn = df_vpn[["EMAIL_ADDRESS", "VPN_LAST_LOGON_DATE"]]

    # df_gs = c.getGSuite()
    df_gs = df_gsuite[["EMAIL_ADDRESS", "GS_LAST_LOGON_DATE"]]

    # df_sm = c.getSurgeMail()
    df_sm = df_surge[["EMAIL_ADDRESS", "SM_LAST_LOGON_DATE"]]

    # df_sm = getSurgeMail()

    df_ad = pd.merge(left=df_ad, right=df_vpn, how='left', left_on='EMAIL_ADDRESS', right_on='EMAIL_ADDRESS')
    df_ad = pd.merge(left=df_ad, right=df_gs, how='left', left_on='EMAIL_ADDRESS', right_on='EMAIL_ADDRESS')
    df_ad = pd.merge(left=df_ad, right=df_sm, how='left', left_on='EMAIL_ADDRESS', right_on='EMAIL_ADDRESS')
    # df_nomina = pd.merge(left=df_nomina, right=df_sm, how='left', left_on='EMAIL_ADDRESS', right_on='EMAIL_ADDRESS')

    # Convierto las columnas de fecha a formato date (sinó el cálculo del max, da error porque no saca max de strings).
    df_ad["AD_CREATION_DATE"] = pd.to_datetime(df_ad["AD_CREATION_DATE"], format="%d/%m/%Y")
    df_ad["AD_LAST_LOGON_DATE"] = pd.to_datetime(df_ad["AD_LAST_LOGON_DATE"], format="%d/%m/%Y %H:%M")
    df_ad["VPN_LAST_LOGON_DATE"] = pd.to_datetime(df_ad["VPN_LAST_LOGON_DATE"], format="%d/%m/%Y %H:%M")
    df_ad["GS_LAST_LOGON_DATE"] = pd.to_datetime(df_ad["GS_LAST_LOGON_DATE"], format="%d/%m/%Y %H:%M:%S")
    df_ad["SM_LAST_LOGON_DATE"] = pd.to_datetime(df_ad["SM_LAST_LOGON_DATE"], format="%d/%m/%Y %H:%M:%S")

    print(" "); print("df_ad (formateadas las fechas como date):"); print(df_ad) #; print(df_ad.info())

    # Calculo el last_logon como el last_logon de todas las app
    print("+ Calcula el Max(Last_Logon_Date)")
    df_ad["MAX_LAST_LOGON_DATE"] = df_ad[["AD_CREATION_DATE", "AD_LAST_LOGON_DATE", "VPN_LAST_LOGON_DATE", "GS_LAST_LOGON_DATE", "SM_LAST_LOGON_DATE"]].max(axis=1, skipna=True)
    # df_ad["MAX_LAST_LOGON"] = np.nanmax(df_ad["AD_CREATION_DATE", "AD_LAST_LOGON_DATE", "VPN_LAST_LOGON_DATE", "GS_LAST_LOGON_DATE"].values, axis=0) # este método es más rápido que el anterior.

    # Calculo el AD_ELAPSED_DAYS
    df_ad["MAX_ELAPSED_DAYS"] = df_ad["MAX_LAST_LOGON_DATE"].apply(lambda x: abs(datetime.now() - x).days)

    print(" "); print("df_ad (con Max_Last_Logon):"); print(df_ad)
    f.quick_excel(df_ad, "Rep_AD_Max_Last_Logon")
    print("- Calcula el Max(Last_Logon_Date)")

    # Formateo las fechas.
    df_ad["MAX_LAST_LOGON_DATE"] = df_ad["MAX_LAST_LOGON_DATE"].apply(lambda x: f.formateaFecha(str(x), '%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M'))
    df_ad["AD_CREATION_DATE"] = df_ad["AD_CREATION_DATE"].apply(lambda x: f.formateaFecha(str(x), '%Y-%m-%d', '%d/%m/%Y'))
    df_ad["AD_LAST_LOGON_DATE"] = df_ad["AD_LAST_LOGON_DATE"].apply(lambda x: f.formateaFecha(str(x), '%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M'))
    df_ad["VPN_LAST_LOGON_DATE"] = df_ad["VPN_LAST_LOGON_DATE"].apply(lambda x: f.formateaFecha(str(x), '%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M'))
    df_ad["GS_LAST_LOGON_DATE"] = df_ad["GS_LAST_LOGON_DATE"].apply(lambda x: f.formateaFecha(str(x), '%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M'))
    df_ad["SM_LAST_LOGON_DATE"] = df_ad["SM_LAST_LOGON_DATE"].apply(lambda x: f.formateaFecha(str(x), '%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M'))

    # Borro las columnas de fecha que agregué para hacer el cálculo (porque después quedan como _x e _y en los merge de cada apps).
    # df_ad.drop(["VPN_LAST_LOGON_DATE", "GS_LAST_LOGON_DATE"], axis='columns', inplace=True)

    # -----------------------------------------------------------
    # Genera el reporte
    # -----------------------------------------------------------
    print(" "); print("df_ad (final):"); print(df_ad)
    f.quick_excel(df_ad, filename, final=True, put_sftp=self.envia_sftp)

    # self.w.wclose()

    print("- RUN")
    print("Processing elapsed time {}".format(datetime.now() - start))
    print("+ REPORTE AD INACTIVITY")
