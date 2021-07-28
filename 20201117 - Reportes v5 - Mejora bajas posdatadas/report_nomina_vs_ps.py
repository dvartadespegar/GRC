import pandas as pd
import numpy as np
import funcs as f
from datetime import datetime
from configparser import ConfigParser

#pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


def rep_nomina_vs_ps(self, completo=False, envia_sftp=False, args=()):
    print("+ REPORT NOMINA VS VPN")
    print("Params: completo =", completo, "; envia_sftp =", envia_sftp)
    start = datetime.now()

    df_nomina = args[0]

    # ----------------------------------------
    # PS
    # ----------------------------------------
    cparser = ConfigParser()
    cparser.read("params/users.conf")
    c_ps_path = cparser.get("RRHH", "File Location") + "/Usuarios PS.xls"
    # df_ps = pd.read_excel("C:/Users/daniel.vartabedian/Box Sync/DESPEGAR/Terminados/2019/20171127 - IDM - midPoint/Proyecto IDM/RRHH/Usuarios PS.xls")
    df_ps = pd.read_excel(c_ps_path)

    # Elimino la columna Rol
    df_ps.drop("Rol", axis=1, inplace=True)
    # Renombro campos y elimino duplicados (el archivo original viene con 1 registro por rol).
    df_ps.rename(columns={"Usuario": "PS_USER_NAME", "Email": "PS_EMAIL_ADDRESS", "Última Conexión": "PS_LAST_LOGON_DATE", "Última Actualización": "PS_LAST_UPDATE_DATE"}, inplace=True)
    df_ps.sort_values("PS_USER_NAME", inplace=False)
    df_ps.drop_duplicates(subset=None, keep="first", inplace=True)

    print(" "); print("df_ps (original) ="); print(df_ps)

    df_ps["PS_LAST_LOGON_DATE"] = df_ps["PS_LAST_LOGON_DATE"].apply(lambda x: f.formateaFecha((str(x)), '%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M'))
    df_ps["PS_LAST_UPDATE_DATE"] = df_ps["PS_LAST_UPDATE_DATE"].apply(lambda x: f.formateaFecha((str(x)), '%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M'))

    print(" "); print("df_ps (original con fechas) ="); print(df_ps)


    # ------------------------------------------------------------------------------------------------------
    # Hago el merge con PS.
    # ------------------------------------------------------------------------------------------------------
    df = pd.merge(left=df_nomina, right=df_ps, how='right', left_on='EMAIL_ADDRESS', right_on='PS_EMAIL_ADDRESS')

    # Cargo el Origen para aquellos registros que lo tienen vacío (porque son los únicos que no vienen de nómina, luego son de la app).
    df["ORIGEN"] = None
    df.loc[pd.isna(df["ORIGEN"]), "ORIGEN"] = "PS"

    print(" "); print("df (Merged):"); print(df)
    f.quick_excel(df, "Rep_Merged")

    # ------------------------------------------------------------------------------------------------------
    # Agrego la columna de comentarios:
    # ------------------------------------------------------------------------------------------------------
    print("+ Cargo los comentarios")

    # Convierto la fecha en campo date, además la copio en otro campo para evitar el error de slice.
    df["NEW_END_DATE"] = df.loc[:, "END_DATE"]
    df['NEW_END_DATE'] = pd.to_datetime(df['NEW_END_DATE'], format='%d/%m/%Y')

    df["COMMENTS"] = None
    cond =  [   # No está en RRHH ni en Opecus, y tiene PS Activo (sólo PS)
                # (pd.isna(df["COMMENTS"])) &
                (pd.isna(df["FULL_NAME"])) & (pd.isna(df["OPQ_FULL_NAME"])) & (df["PS_USER_NAME"]),

                # No está en RRHH, está en Opecus Desactivado y en PS Activo (sólo Opecus).
                # (pd.isna(df["COMMENTS"])) &
                (pd.isna(df["FULL_NAME"])) & (pd.isna(df["OPQ_FULL_NAME"]) == False) &
                (df["OPQ_STATUS"] == "Desactivado") & (df["PS_USER_NAME"]),

                # Está Inactivo en RRHH, no está en Opecus y tiene PS Activo (sólo RRHH).
                # (pd.isna(df["COMMENTS"])) &
                (pd.isna(df["END_DATE"]) == False) & (len(str(df["END_DATE"])) > 7) &
                (df["NEW_END_DATE"] < datetime.now()) &
                (pd.isna(df["OPQ_FULL_NAME"])) & (df["PS_USER_NAME"]),

                # Está Inactivo en RRHH, está en Opecus Desactivado y en VPN Activo.
                # (pd.isna(df["COMMENTS"])) &
                (pd.isna(df["END_DATE"]) == False) & (len(str(df["END_DATE"])) > 7) &
                (df["NEW_END_DATE"] < datetime.now()) &
                (pd.isna(df["OPQ_FULL_NAME"]) == False) & (df["OPQ_STATUS"] == "Desactivado") &
                (df["PS_USER_NAME"]),
        ]
    result = ["Dar de baja PS (sólo PS)", "Dar de baja PS (sólo Opecus)", "Dar de baja PS (sólo RRHH)", "Dar de baja PS"]
    df["COMMENTS"] = np.select(cond, result, default=df["COMMENTS"])
    print("- Cargo los comentarios")

    # Ordeno la salida
    df.sort_values(['COMMENTS', 'EMAIL_ADDRESS'], ascending=[False, True], na_position='last', inplace=True)

    print(" "); print("df (Merged con comentarios):"); print(df)

    # ------------------------------------------------------
    # Dejo sólo las filas con Comentarios
    # ------------------------------------------------------
    print("+ Filtro bajas")
    # df2 = df[pd.isna(df["COMMENTS"]) == False]  # esto debería estar activo, pero con tan pocos registros, queda un poco más claro cuál es el universo de usuarios de PS.
    df2 = df
    print("- Filtro bajas")

    # Limpio los valores nulos (ojo! después de esto, ya no puedo preguntar más por is null).
    print("+ Limpio los df")
    df = df.fillna("", inplace=False)
    df2 = df2.fillna("", inplace=False)
    print("- Limpio los df")

    # ----------------------------------------------------------------------------------------------------------------
    # Dejo sólo las columnas que me interesan y les cambio el orden (para mandar el df al QTableWidget de pantalla).
    # ----------------------------------------------------------------------------------------------------------------
    df2 = df2[["PS_USER_NAME", "PS_EMAIL_ADDRESS", "PS_LAST_LOGON_DATE", "PS_LAST_UPDATE_DATE",
               "FULL_NAME", "END_DATE", "OPQ_USERNAME", "OPQ_FULL_NAME", "OPQ_END_DATE",
               "AD_SAMACCOUNTNAME", "AD_STATUS", "AD_LAST_LOGON_DATE",
               "ORIGEN", "COMMENTS"]]

    if self.completo:
        print(" "); print("df (final):"); print(df)
        f.quick_excel(df, "Reporte_Nomina_vs_PS (Full)", final=True, put_sftp=self.envia_sftp)
    else:
        print(" "); print("df2 (final):"); print(df2)
        f.quick_excel(df2, "Reporte_Nomina_vs_PS", final=True, put_sftp=self.envia_sftp)

    print("Processing elapsed time {}".format(datetime.now() - start))
    print("- REPORT NOMINA VS PS")
