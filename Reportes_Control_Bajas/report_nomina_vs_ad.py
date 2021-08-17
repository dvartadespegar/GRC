import pandas as pd
import numpy as np
import threading
import funcs as f
from datetime import datetime
import connections as c
from configparser import ConfigParser

#pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


def rep_nomina_vs_ad(self, completo=False, envia_sftp=False, args=()):
    print(" "); print("+ REPORTE NOMINA VS AD")
    print("Params: completo =", completo, "; envia_sftp =", envia_sftp)
    start = datetime.now()

    df_ad_actives = args[0]
    df_nomina = args[1]

    print(" "); print("df_ad_actives (original):"); print(df_ad_actives)
    f.quick_excel(df_ad_actives, "Rep_AD_Actives (original)")


    #     hilo = MiHilo(args=(completo, envia_sftp, ), daemon=False)
#     print("- MiHilo")
#     hilo.start()
#
#     print("isAlive? (después del while) = ", hilo.is_alive())
#
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
#         self.prog.setTitle("Reporte Nómina vs AD (Completo)" if self.completo else "Reporte Nómina vs AD")
#         self.prog.show()
#
#     def run(self):
#         dt_start = datetime.datetime.now()
#         self.prog.adv(5)

    """
    # ----------------------------------------------
    # RRHH
    # ----------------------------------------------
    df_csv = c.getCSV()

    print(" "); print("df_csv (rrhh):"); print(df_csv)
    f.quick_excel(df_csv, "Rep_RRHH")
    # ----------------------------------------------
    # Opecus
    # ----------------------------------------------
    opq = c.getOpecus()
    df_opq = opq.get_actives()
    df_opq_inact = opq.get_inactives()
    opq.close()
    df_opq = pd.concat([df_opq, df_opq_inact])

    df_opq.sort_values(['EMAIL_ADDRESS', 'OPQ_STATUS'], ascending=[True, True], na_position='last', inplace=True)
    df_opq.drop_duplicates(subset="EMAIL_ADDRESS", keep='first', inplace=True)   # de los duplicados, dejo sólo los Activados

    print(" "); print("df_opq:"); print(df_opq)
    f.quick_excel(df_opq, "Rep_Opecus")

    # ==============================================
    # Genero el df_nomina: concateno RRHH + Opecus
    # ==============================================
    df_nomina = pd.concat([df_csv, df_opq])

    print(" "); print("df_nomina (RRHH + Opecus):"); print(df_nomina)
    f.quick_excel(df_nomina, "Rep_Concat (RRHH-Opecus)")

    # --------------------------------------------------------------------------------------------------
    # Traigo el AD (dejo 1 solo registro activo, porque un mismo usuario puede tener varios registros).
    # --------------------------------------------------------------------------------------------------
    ad = c.getAD()
    df_ad = ad.get_actives()

    # OJO! esto es nuevo: lo saco porque más abajo estoy eliminando los registros que están DISABLED;
    # luego, no tiene sentido buscar los inactivos si después los voy a sacar. DV-06/10/2020.
    # df_ad_inact = ad.get_inactives()
    # df_ad = pd.concat([df_ad, df_ad_inact])

    df_ad.sort_values(['EMAIL_ADDRESS', 'AD_LAST_LOGON_DATE'], ascending=[True, True], na_position='last', inplace=True)
    df_ad.drop_duplicates(subset="EMAIL_ADDRESS", keep='first', inplace=True)

    print(" "); print("df_ad:"); print(df_ad)
    f.quick_excel(df_ad, "Rep_AD")
    """

    # ==============================================
    # Genero el df con nómina + AD
    # ==============================================
    # df = pd.merge(left=df_nomina, right=df_ad_actives, how='outer', left_on='EMAIL_ADDRESS', right_on='EMAIL_ADDRESS')
    # df = pd.merge(left=df_concat, right=df_ad, how='left', left_on='EMAIL_ADDRESS', right_on='EMAIL_ADDRESS')

    # Agrego el df_ad_actives: en df_nomina ya están los datos del ad, pero para agregarlos se hizo un inner join
    # para que se crucen sólo las coincidencias (necesario para todos los reportes). En este reporte necesito además agregar
    # los registros del AD que no coinciden, pero no viene así de nomina(), así que lo agrego acá.
    # No uso un merge (que tiene mejor desempeño de memoria) para no estar cambiando el nombre de los campos _x y _y que se van a generar.
    df_nomina = pd.concat([df_nomina, df_ad_actives])

    # Si bien agrego el df_ad_actives completo, tengo que borrar aquellos registros que están con su samaccountname duplicado,
    # y dejo el primero, que era el original (es el que vino de nómina y está mergeado con rrhh y opecus).
    df_nomina.drop_duplicates(subset=['AD_SAMACCOUNTNAME'], inplace=True, keep='first')

    df = df_nomina

    # Agrego al campo ORIGEN el valor AD, porque lo que vino del df_concat ya lo tiene, pero lo que vino del AD no.
    df.loc[pd.isna(df["ORIGEN"]), "ORIGEN"] = "AD"

    print(" "); print("df (merge df_nomina + df_ad_actives):"); print(df)
    f.quick_excel(df, "Rep_Nomina_vs_AD (original)")
    # self.prog.adv(50)

    # ----------------------------------------------------------------------
    # Dejo en df sólo los registros con AD activo
    # Esto lo saco por el mismo motivo explicado más arriba. DV-06/10/2020.
    # ----------------------------------------------------------------------
    # Esto deja un df con True en las filas donde se cumple la condición.
    # df_x = df["AD_STATUS"].apply(lambda x: str(x).upper().find("DISABLE") < 0)
    # Con esto dejo en df_nomna aquellos registros de df que tengan True en df_x
    # df = df[df_x]

    # ----------------------------------------------------
    # Saco los registros que no tienen el sAMAccountName.
    # ----------------------------------------------------
    df_x = (pd.isna(df["AD_SAMACCOUNTNAME"]) == False)
    df = df[df_x]

    df.reset_index(drop=True, inplace=True)        # Esto es necesario para iniciar el nro fila en 0; hacerlo después de eliminar registros del df

    print(" "); print("df (nomina_vs_ad) - (final antes de comentarios)"); print(df)
    f.quick_excel(df, "Rep_Nomina_vs_AD-sin comments")

    # ==============================================
    # Cargo los comentarios
    # ==============================================
    # Convierto la fecha en campo date, además la copio en otro campo para evitar el error de slice.
    df["NEW_END_DATE"] = df.loc[:, "END_DATE"]
    df['NEW_END_DATE'] = pd.to_datetime(df['NEW_END_DATE'], format='%d/%m/%Y')

    df["COMMENTS"] = None
    print("+ Cargo los comentarios")
    cond =  [   # No está en RRHH ni en Opecus, y tiene AD Activo (sólo AD)
                (pd.isna(df["COMMENTS"])) &
                (pd.isna(df["FULL_NAME"])) & (pd.isna(df["OPQ_FULL_NAME"])) &
                (pd.isna(df["AD_STATUS"]) == False) & (~df["AD_STATUS"].str.upper().str.contains("DISABLE")),
                # No está en RRHH, está en Opecus Desactivado y en AD Activo (sólo Opecus).
                (pd.isna(df["COMMENTS"])) &
                (pd.isna(df["FULL_NAME"])) &
                (pd.isna(df["OPQ_FULL_NAME"]) == False) & (df["OPQ_STATUS"] == "Desactivado") &
                (pd.isna(df["AD_STATUS"]) == False) & (~df["AD_STATUS"].str.upper().str.contains("DISABLE")),
                # Está Inactivo en RRHH, no está en Opecus y tiene AD Activo (sólo RRHH).
                (pd.isna(df["COMMENTS"])) &
                (pd.isna(df["END_DATE"]) == False) & (len(str(df["END_DATE"])) > 7) &
                (df["NEW_END_DATE"] < datetime.now()) &
                (pd.isna(df["OPQ_FULL_NAME"])) &
                (pd.isna(df["AD_STATUS"]) == False) & (~df["AD_STATUS"].str.upper().str.contains("DISABLE")),
                # (pd.isna(df["AD_STATUS"]) == False) & (str(df["AD_STATUS"]).upper().find("DISABLE") < 0),
                # Está Inactivo en RRHH, está en Opecus Desactivado y en AD Activo.
                (pd.isna(df["COMMENTS"])) &
                (pd.isna(df["END_DATE"]) == False) & (len(str(df["END_DATE"])) > 7) &
                (df["NEW_END_DATE"] < datetime.now()) &
                (pd.isna(df["OPQ_FULL_NAME"]) == False) & (df["OPQ_STATUS"] == "Desactivado") &
                (pd.isna(df["AD_STATUS"]) == False) & (~df["AD_STATUS"].str.upper().str.contains("DISABLE")),
    ]
    result = ["Dar de baja AD (Sólo AD)", "Dar de baja AD (sólo Opecus)", "Dar de baja AD (sólo RRHH)", "Dar de baja AD"]
    df["COMMENTS"] = np.select(cond, result, default=df["COMMENTS"])
    print("- Cargo los comentarios")

    # Ordeno la salida
    df.sort_values(['COMMENTS', 'EMAIL_ADDRESS'], ascending=[False, True], na_position='last', inplace=True)

    print(" "); print("df (Merged con comentarios):"); print(df)

    # ------------------------------------------------------
    # Dejo sólo las filas con Comentarios.
    # ------------------------------------------------------
    print("+ Filtro bajas")
    df2 = df[pd.isna(df["COMMENTS"]) == False]
    print("- Filtro bajas")
    # self.prog.adv(80)

    # ----------------------------------------------------------------------------------------------------------------
    # Dejo sólo las columnas que me interesan
    # ----------------------------------------------------------------------------------------------------------------
    df2 = df2[["FULL_NAME", "EMAIL_ADDRESS", "END_DATE", "OPQ_USERNAME", "OPQ_FULL_NAME", "OPQ_END_DATE",
               "AD_SAMACCOUNTNAME", "AD_STATUS", "AD_LAST_LOGON_DATE", "ORIGEN", "COMMENTS"]]
    # self.prog.adv(90)

    if completo:
        f.quick_excel(df, "Reporte_Nomina_vs_AD (Full)", final=True, put_sftp=envia_sftp)
    else:
        f.quick_excel(df2, "Reporte_Nomina_vs_AD", final=True, put_sftp=envia_sftp)

    print("Processing elapsed time {}".format(datetime.now() - start))
    print("- REPORTE NOMINA VS AD")
