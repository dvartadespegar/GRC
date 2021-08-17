import pandas as pd
import numpy as np
import funcs as f
from datetime import datetime

#pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


def rep_nomina_vs_gsuite(self, completo=False, envia_sftp=False, args=()):
    print(" "); print("+ REPORTE NOMINA VS GSUITE")
    print("Params: completo =", completo, "; envia_sftp =", envia_sftp)
    start = datetime.now()

    df_gsuite = args[0]
    df_nomina = args[1]

    # ------------------------------------------------------------------------------------------------------
    # Hago el merge entre Nómina y GSuite.
    # ------------------------------------------------------------------------------------------------------
    df = pd.merge(left=df_nomina, right=df_gsuite, how='right', left_on='EMAIL_ADDRESS', right_on='EMAIL_ADDRESS')

    # Cargo el Origen para aquellos registros que lo tienen vacío (porque son los únicos que no vienen de nómina, luego son de la app).
    df.loc[pd.isna(df["ORIGEN"]), "ORIGEN"] = "GSUITE"

    print(" "); print("df (Merged):"); print(df)
    f.quick_excel(df, "Rep_GS_Merged")

    # ------------------------------------------------------------------------------------------------------
    # Cargo la columna de comentarios (esta columna se crea al generar la nómina):
    # ------------------------------------------------------------------------------------------------------
    print("+ Cargo los comentarios")

    # Convierto la fecha en campo date, además la copio en otro campo para evitar el error de slice.
    df["NEW_END_DATE"] = df.loc[:, "END_DATE"]
    df['NEW_END_DATE'] = pd.to_datetime(df['NEW_END_DATE'], format='%d/%m/%Y')

    df["COMMENTS"] = None
    cond =  [   # No está en RRHH ni en Opecus, y tiene GS Activo (sólo GS)
                (pd.isna(df["FULL_NAME"])) & (pd.isna(df["OPQ_FULL_NAME"])) & (pd.isna(df["GS_FULL_NAME"]) == False),

                # No está en RRHH, está en Opecus Desactivado y en GS Activo (sólo Opecus).
                (pd.isna(df["FULL_NAME"])) & (pd.isna(df["OPQ_FULL_NAME"]) == False) &
                (df["OPQ_STATUS"] == "Desactivado") & (pd.isna(df["GS_FULL_NAME"]) == False),

                # Está Inactivo en RRHH, no está en Opecus y tiene GS Activo (sólo RRHH).
                (pd.isna(df["END_DATE"]) == False) & (len(str(df["END_DATE"])) > 7) &
                (df["NEW_END_DATE"] < datetime.now()) &
                (pd.isna(df["OPQ_FULL_NAME"])) & (pd.isna(df["GS_FULL_NAME"]) == False),

                # Está Inactivo en RRHH, está en Opecus Desactivado y en GS Activo.
                (pd.isna(df["END_DATE"]) == False) & (len(str(df["END_DATE"])) > 7) &
                (df["NEW_END_DATE"] < datetime.now()) &
                (pd.isna(df["OPQ_FULL_NAME"]) == False) & (df["OPQ_STATUS"] == "Desactivado") &
                (pd.isna(df["GS_FULL_NAME"]) == False),
        ]
    result = ["Dar de baja GSuite (sólo GSuite)", "Dar de baja GSuite (sólo Opecus)", "Dar de baja GSuite (sólo RRHH)", "Dar de baja GSuite"]
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

    # ----------------------------------------------------------------------------------------------------------------
    # Dejo sólo las columnas que me interesan
    # ----------------------------------------------------------------------------------------------------------------
    df2 = df2[["FULL_NAME", "EMAIL_ADDRESS", "END_DATE", "OPQ_USERNAME", "OPQ_FULL_NAME", "OPQ_END_DATE",
               "AD_SAMACCOUNTNAME", "AD_STATUS", "AD_LAST_LOGON_DATE",
               "GS_ORG_UNIT", "GS_LAST_LOGON_DATE", "ORIGEN", "COMMENTS"]]

    if completo:
        f.quick_excel(df, "Reporte_Nomina_vs_GSuite (Full)", final=True, put_sftp=self.envia_sftp)
    else:
        f.quick_excel(df2, "Reporte_Nomina_vs_GSuite", final=True, put_sftp=self.envia_sftp)

    print("Processing elapsed time {}".format(datetime.now() - start))
    print("- REPORTE NOMINA VS GSUITE")
