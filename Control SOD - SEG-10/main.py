# ==============================================================================================
#
# Purpose: Generar una planilla donde se informen todas las incompatibilidades por usuario.
#
# Notes:   Se cruzan roles de Oracle, ATP1 y ATP3, contra la matriz de incompatibilidades
#          provista por Auditoría.
#
# History: 12/08/2020 DVartabedian Created.
#          15/11/2021 DVartabedian Se agrega log del proceso para evidencia del control.
#
# ==============================================================================================
print("+ Importación de módulos")
import os
import platform
import logging
from datetime import datetime
import pandas as pd
import numpy as np
import connections as c
import funcs as f
print("- Importación de módulos")

print("+ Configura logging")
if platform.platform().startswith('Windows'):
    log_filename = os.path.join(os.getcwd(), 'log_' + datetime.now().strftime("%Y%m%d_%H%M%S") + ".log")
else:
    log_filename = os.path.join(os.getenv('HOME'), 'log_' + datetime.now().strftime("%Y%m%d_%H%M%S") + ".log")
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)-8s %(name)-12s : %(message)s',
#                            format='%(asctime)s %(threadName)-10s %(levelname)-8s %(name)-17s : %(message)s',
                    datefmt='%d/%m %H:%M:%S',
                    filename=log_filename,
                    filemode="w")
logging.info("============/ START: " + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + " /============")
print("- Configura logging")


class Main():
    def __init__(self):
        print("+ Pandas Setup")
        # pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', None)
        print("- Pandas Setup")

        # Debugging flags
        self.with_sod_matrix = False   # genera el reporte Rep_SOD_Matrix.xlsx
        self.with_concat = False       # genera el reporte Rep_Concat.xlsx
        self.with_debug = False        # genera reportes en cada etapa intermedia (es más lento)


    def quit(self):
        quit()

    def process(self, filename):
        # -----------------------------------------------------------
        logging.info("+ Get SOD matrix")
        # -----------------------------------------------------------
        df_sod = c.get_SOD(filename)
        print(" "); print("df_sod (final) = "); print(df_sod)
        if self.with_sod_matrix:
            f.quick_excel(df_sod, "Rep_SOD_Matrix")
        logging.info("- Get SOD matrix; " + str(df_sod.shape))

        # -----------------------------------------------------------
        logging.info("+ Get Oracle Users & Responsibilities")
        # -----------------------------------------------------------
        ora = c.getOracle()
        df_ora = ora.get_full_user_resp()
        print(" "); print("df_ora = "); print(df_ora)
        logging.info("- Get Oracle Users & Responsibilities; " + str(df_ora.shape))

        # -----------------------------------------------------------
        logging.info("+ Get ATP1")
        # -----------------------------------------------------------
        atp1 = c.getATP1()
        df_atp1 = atp1.get_full_user_role()
        print(" "); print("df_atp1 = "); print(df_atp1)
        # f.quick_excel(df_atp1, "Rep_ATP1")
        logging.info("- Get ATP1; " + str(df_atp1.shape))

        # -----------------------------------------------------------
        logging.info("+ Get ATP3")
        # -----------------------------------------------------------
        atp3 = c.getATP3()
        df_atp3 = atp3.get_full_user_role()
        print(" "); print("df_atp3 = "); print(df_atp3)
        logging.info("- Get ATP3; " + str(df_atp3.shape))

        # -----------------------------------------------------------
        logging.info("+ Concatenate Oracle, ATP1 & ATP3")
        # -----------------------------------------------------------
        df_concat = pd.concat([df_ora, df_atp3, df_atp1])
        df_concat.dropna(subset=['EMAIL_ADDRESS'], axis=0)
        df_concat.reset_index(drop=True, inplace=True)
        print(" "); print("df_concat = "); print(df_concat)
        if self.with_concat:
            f.quick_excel(df_concat, "Rep_Concat", extension='xlsx')
        logging.info("- Concatenate Oracle, ATP1 & ATP3; " + str(df_concat.shape))

        # -----------------------------------------------------------
        logging.info("+ Incompatibilities Process")
        # --------------------------------------------------------------------------------------
        # El df_tmp1 debería traer todo lo que tiene el usuario, cruzado con la matriz sod;
        # es decir, si algún rol tiene incompatibilidades, lo trae en este df; luego habrá
        # que ver si esas incompatibilidades también las tiene el usuario: ésta es la cond
        # por la cual deben aparecer los registros en el reporte final.
        # --------------------------------------------------------------------------------------
        # Le agregué el "left" para debugging, pero no debería ser necesario (porque lo que no matchea no lo debería necesitar porque no tiene incompatibilidades).
        # df_tmp1 = pd.merge(df_concat, df_sod, how="left", on=["APPL_NAME", "ROLE_NAME"])  # por cada respo/rol trae todas sus incompatibilidades

        logging.info("  + Merge Concat with SOD Matrix (get User Roles and its Incompatibilities)")
        # df_tmp1 = pd.merge(left=df_concat, how="inner", right=df_sod, on=["APPL_NAME", "ROLE_NAME"], suffixes=["_con", "_sod"])  # por cada respo/rol trae todas sus incompatibilidades
        df_tmp1 = pd.merge(left=df_concat, right=df_sod, how="inner",
                           left_on=["APPL_NAME", "MODULE_NAME", "ROLE_NAME"],
                           right_on=["APPL_NAME", "MODULE_NAME", "ROLE_NAME"],
                           suffixes=["_usr", "_sod"])
        print(" "); print("df_tmp1 (df_concat + df_sod) = "); print(df_tmp1)
        df_tmp1 = df_tmp1.sort_values(["EMAIL_ADDRESS", "APPL_NAME"])
        if self.with_debug:
            f.quick_excel(df_tmp1, "Rep_Proceso1", extension='xlsx')
        logging.info("  - Merge Concat with SOD Matrix (get User Roles and its Incompatibilities); " + str(df_tmp1.shape))

        # --------------------------------------------------------------------------------------
        # El df_tmp1 tiene los roles del usuario y las incompatibilidades previstas para cada rol;
        # el df_tmp3 es el cruce de esas incompatibilidades con los roles asignados al usuario,
        # para ver si tiene alguna de esas incompatibilidades. Si las tiene, va al reporte.
        # --------------------------------------------------------------------------------------

        # A la lista de roles y sus incompatibilidades, lo cruzo con la del usuario y sus respos: cada cruce debería representar 1 incompatibilidad.
        # df_tmp3 = pd.merge(left=df_tmp1, right=df_concat, left_on=["EMAIL_ADDRESS", "APPL_INCOMP_NAME", "INCOMP_ROLE_NAME"],
        #                    right_on=["EMAIL_ADDRESS", "APPL_NAME", "ROLE_NAME"], suffixes=["_inc", "_usr"])

        # esto es una prueba x icartes e ivan_derechinsky; confirmado está ok: con esto trae los cruces de Oracle con ATP1/ATP3 (el merge anterior no los trae).
        # df_tmp3 = pd.merge(left=df_tmp1, right=df_concat, how="inner",
        #                    left_on=["EMAIL_ADDRESS", "INCOMP_ROLE_NAME"],
        #                    right_on=["EMAIL_ADDRESS", "ROLE_NAME"], suffixes=["_inc", "_usr"])
        # print(" "); print("df_tmp3 (original) = "); print(df_tmp3)

        logging.info("  + Merge User Roles and its Incompatibilities with Concat (get Incompatibilities User Roles)")
        # esto es otra prueba: para jyamamoto cruza los roles con los módulos
        df_tmp3 = pd.merge(left=df_tmp1, right=df_concat, how="inner",
                           left_on=["EMAIL_ADDRESS", "APPL_INCOMP_NAME", "INCOMP_MODULE_NAME", "INCOMP_ROLE_NAME"],
                           right_on=["EMAIL_ADDRESS", "APPL_NAME", "MODULE_NAME", "ROLE_NAME"], suffixes=["_inc", "_usr"])
        print(" "); print("df_tmp3 (original) = "); print(df_tmp3.shape); print(df_tmp3)

        df_tmp3 = df_tmp3[["EMAIL_ADDRESS", "USER_NAME_usr", "APPL_NAME_inc", "MODULE_NAME_inc", "ROLE_NAME_inc", "ROLE_COUNTRY_inc", "APPL_INCOMP_NAME", "INCOMP_MODULE_NAME", "INCOMP_ROLE_NAME", "ROLE_COUNTRY_usr", "COMBINATION", "EMPLOYEE_COUNTRY_usr", "SUPERVISOR_usr", "SUPER_SUPERVISOR_usr"]]
        df_tmp3 = df_tmp3.rename(columns={"USER_NAME_usr": "USER_NAME", "APPL_NAME_inc": "APPL_NAME", "MODULE_NAME_inc": "MODULE_NAME", "ROLE_NAME_inc": "ROLE_NAME", "ROLE_COUNTRY_inc": "COUNTRY_RESP", "ROLE_COUNTRY_usr": "COUNTRY_INCOMP", "EMPLOYEE_COUNTRY_usr": "EMPLOYEE_COUNTRY", "SUPERVISOR_usr": "SUPERVISOR", "SUPER_SUPERVISOR_usr": "SUPER_SUPERVISOR"})
        df_tmp3 = df_tmp3.sort_values(["EMAIL_ADDRESS", "ROLE_NAME"])

        print(" "); print("df_tmp3 (final) = "); print(df_tmp3)
        logging.info("  - Merge User Roles and its Incompatibilities with Concat (get Incompatibilities User Roles); " + str(df_tmp3.shape))
        if self.with_debug:
            f.quick_excel(df_tmp3, "Rep_Proceso3")
        logging.info("- Incompatibilities Process; " + str(df_tmp3.shape))

        # -----------------------------------------------------------
        logging.info("+ Cleaning duplicates")
        # -----------------------------------------------------------
        print("  Cantidad de registros antes de remover duplicados: ", len(df_tmp3))

        # Con esto borro las combinaciones donde ["Rol_1", "Rol_2"] == ["Rol_2", "Rol_1"] -

        cols = ['ROLE_NAME','INCOMP_ROLE_NAME']

        df_fin = df_tmp3.assign(**pd.DataFrame(np.sort(df_tmp3[cols], axis=1), columns=cols, index=df_tmp3.index))
        print(" "); print("df_fin (original) = "); print(df_fin)
        if self.with_debug:
            f.quick_excel(df_fin, "Rep_Proceso4")
        df_fin = df_fin.drop_duplicates(['EMAIL_ADDRESS'] + cols) #.sort_values(["EMAIL_ADDRESS","APPL_NAME","ROLE_NAME"])
        df_fin.reset_index(drop=True, inplace=True)

        print(" "); print("df_fin (sin duplicados):"); print(df_fin)
        if self.with_debug:
            f.quick_excel(df_fin, "Rep_Proceso5")
        print("  Cantidad de registros después de remover duplicados: ", len(df_fin))
        logging.info("- Cleaning duplicates; " + str(df_fin.shape))

        # -----------------------------------------------------------
        logging.info("+ Checking falses positives (when the incompatible role is in a different country)")
        # -----------------------------------------------------------
        # Si la incompatibilidad está en Oracle, reviso que la respo sea del mismo país (resp_name vs incomp_resp_name).
        df_final = df_fin[((df_fin["APPL_NAME"] == 'ORACLE') & (df_fin["APPL_INCOMP_NAME"] == 'ORACLE') & (df_fin["COUNTRY_RESP"] == df_fin["COUNTRY_INCOMP"])) |
                          ((df_fin["APPL_INCOMP_NAME"] == 'ORACLE') & (df_fin["APPL_NAME"] == 'ORACLE') & (df_fin["COUNTRY_RESP"] == df_fin["COUNTRY_INCOMP"])) |
                          (df_fin["APPL_NAME"] == 'ATP1') |
                          (df_fin["APPL_NAME"] == 'ATP3') |
                          ((df_fin["APPL_NAME"] == 'ORACLE') & (df_fin["APPL_INCOMP_NAME"] != 'ORACLE')) |
                          ((df_fin["APPL_NAME"] != 'ORACLE') & (df_fin["APPL_INCOMP_NAME"] == 'ORACLE'))]

        print(" "); print("df_final (sin falsos positivos) = "); print(df_final)
        if self.with_debug:
            f.quick_excel(df_final, "Rep_Proceso7")
        logging.info("- Checking falses positives (when the incompatible role is in a different country); " + str(df_final.shape))

        # -----------------------------------------------------------
        logging.info("+ Get Supervisor for each User")
        # -----------------------------------------------------------
        df_csv = c.getCSV()
        df_final = pd.merge(left=df_final, right=df_csv, on="EMAIL_ADDRESS", how="left")
        print(" "); print("df_final (con supervisores) = "); print(df_final)
        logging.info("- Get Supervisor for each User; " + str(df_final.shape))

        # -----------------------------------------------------------
        logging.info("+ Generate final spreadsheet")
        # -----------------------------------------------------------
        df_final = df_final[["EMAIL_ADDRESS", "USER_NAME", "APPL_NAME", "MODULE_NAME", "ROLE_NAME", "APPL_INCOMP_NAME", "INCOMP_MODULE_NAME", "INCOMP_ROLE_NAME",
                              "COMBINATION", "FULL_NAME", "SUPERVISOR_NAME", "SUPERVISOR_EMAIL_ADDRESS", "SUPER_SUPERV_NAME", "SUPER_SUPERV_EMAIL", "COUNTRY_CODE"]]
        df_final.sort_values(["EMAIL_ADDRESS","APPL_NAME","ROLE_NAME"], inplace=True)
        df_final.reset_index(drop=True, inplace=True)
        l_sp_name = "SOX-SEG-10 - SOD Compliance Report"
        f.quick_excel(df_final, l_sp_name)
        logging.info("  Spreadsheet Name: " + l_sp_name + ".xlsx, with " + str(df_final.shape[0]) + " records.")
        logging.info("- Generate final spreadsheet; " + str(df_final.shape))


if __name__ == "__main__":
    l_path = os.getcwd().replace(os.sep, '/')
    print("l_path = ", l_path)

    while True:
        print("-------------------------------------------------------------------------------------------")
        print(" Por favor ingrese el nombre de la Matriz SOD a procesar: ")
        filename = input(" Por ejemplo: Updates matriz SOD v.14.xlsx: ")
        filename = l_path + "/" + filename
        print(" Se procesará el archivo: ", filename)
        print("-------------------------------------------------------------------------------------------")
        if os.path.exists(filename):
            break
        else:
            print("Archivo Inexistente - Reintente")

    start = datetime.now()
    myapp = Main()
    myapp.process(filename)
    logging.info("============/ END: " + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + " /============")
    logging.info("Processing elapsed time (report_main): {}".format(datetime.now() - start))
