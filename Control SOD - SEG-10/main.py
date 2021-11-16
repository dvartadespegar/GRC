# ====================================================================================
# Name: sod_matrix.py
# Purpose: Toma la planilla de Auditoría llamada "Updates matriz SOD v.2.xlsx" donde
#          están todas las relaciones de incompatibilidad. Cada función que se cruza
#          es representada por n responsabilidades. Es decir, cada cruce es una
#          multiplicación de un conjunto de respos con otro conjunto de respos que le
#          son incompatibles.
#          Este proceso genera el archivo "sod_matrix.csv" que es el resultado de
#          todas estas incompatibilidades. Este archivo está preparado para ser tomado
#          por un loader en Oracle EBS para después emitir un reporte de incompatiblidades
#          por usuario.
# Notes:
# History: 12/08/2020 DVartabedian Created.
#
# ====================================================================================
print("+ Importa módulos")
import sys
from PyQt5.QtWidgets import QApplication, QMainWindow, QMessageBox
from PyQt5 import uic, QtWidgets
import logging
from datetime import datetime
import pandas as pd
import numpy as np
import connections as c
import funcs as f
print("- Importa módulos")

show_interface = True

# class Main(QMainWindow):
class Main():
    def __init__(self):
        # QMainWindow.__init__(self)
        # uic.loadUi("screen.ui", self)

        print("+ Configura logging")
        logging.basicConfig(level=logging.DEBUG, #INFO,
                            format='%(asctime)s %(levelname)-8s %(name)-12s : %(message)s',
                            datefmt='%d/%m %H:%M:%S')
        print("- Configura logging")

        print("+ Configura pandas")
        # pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', None)
        print("- Configura pandas")

        # if show_interface:
        #     print("+ Abre pantalla")
        #     print("- Abre pantalla")
        #     self.btnOK.clicked.connect(self.process)
        #     self.btnCancel.clicked.connect(self.quit)
        #     self.with_sod_matrix = self.chkSODMatrix
        #     self.with_concat = self.chkConcat
        #     self.with_debug = self.chkDebug
        # else:
        #     self.with_sod_matrix = False
        #     self.with_concat = True
        #     self.with_debug = False

        # Debugging
        self.with_sod_matrix = True   # genera el reporte Rep_SOD_Matrix.xlsx
        self.with_concat = True       # genera el reporte Rep_Concat.xlsx
        self.with_debug = True        # genera reportes en cada etapa intermedia; es muy lento.


    def quit(self):
        quit()

    def process(self):
        # -----------------------------------------------------------
        logging.info("  + Traigo la planilla SOD")
        # -----------------------------------------------------------
        df_sod = c.get_SOD()
        # f.quick_excel(df_sod, "Rep_SOD_Matrix - Original")
        logging.info("  - Traigo la planilla SOD")

        # -----------------------------------------------------------
        logging.info("  + Borro todo lo que NO sea Oracle, ATP1 y ATP3")
        # -----------------------------------------------------------
        df_sod["APPL_NAME"] = df_sod["APPL_NAME"].apply(lambda x: x if x == "ATP1" or x == "ATP3" or x == "ORACLE" else "BORRAR")
        df_sod["APPL_INCOMP_NAME"] = df_sod["APPL_INCOMP_NAME"].apply(lambda x: x if x == "ATP1" or x == "ATP3" or x == "ORACLE" else "BORRAR")
        df_sod.drop(df_sod[df_sod["APPL_NAME"] == "BORRAR"].index, inplace=True)
        df_sod.drop(df_sod[df_sod["APPL_INCOMP_NAME"] == "BORRAR"].index, inplace=True)

        print(" "); print("df_sod (final) = "); print(df_sod)
        if self.with_sod_matrix:
            f.quick_excel(df_sod, "Rep_SOD_Matrix")
        logging.info("  - Borro todo lo que NO sea Oracle, ATP1 y ATP3")

        # ----------------------------------------------------------- #
        logging.info("+ Cargo Oracle")
        # -----------------------------------------------------------
        ora = c.getOracle()
        df_ora = ora.get_full_user_resp()
        print(" "); print("df_ora = "); print(df_ora)
        # f.quick_excel(df_ora, "Rep_Oracle")
        logging.info("- Cargo Oracle")

        # -----------------------------------------------------------
        logging.info("+ Cargo ATP1")
        # -----------------------------------------------------------
        atp1 = c.getATP1()
        df_atp1 = atp1.get_full_user_role()
        print(" "); print("df_atp1 = "); print(df_atp1)
        # f.quick_excel(df_atp1, "Rep_ATP1")
        logging.info("- Cargo ATP1")

        # -----------------------------------------------------------
        logging.info("+ Cargo ATP3")
        # -----------------------------------------------------------
        atp3 = c.getATP3()
        df_atp3 = atp3.get_full_user_role()
        print(" "); print("df_atp3 = "); print(df_atp3)
        # f.quick_excel(df_atp3, "Rep_ATP3")
        logging.info("- Cargo ATP3")

        # -----------------------------------------------------------
        logging.info("+ Concateno los df de las appl")
        # -----------------------------------------------------------
        df_concat = pd.concat([df_ora, df_atp3, df_atp1])
        df_concat.dropna(subset=['EMAIL_ADDRESS'], axis=0)
        df_concat.reset_index(drop=True, inplace=True)
        print(" "); print("df_concat = "); print(df_concat)
        if self.with_concat:
            f.quick_excel(df_concat, "Rep_Concat", extension='xlsx')
        logging.info("- Concateno los df de las appl")

        # -----------------------------------------------------------
        logging.info("+ Proceso incompatibilidaes")
        # -----------------------------------------------------------
        # El df_tmp1 debería traer todo lo que tiene el usuario, cruzado con la matriz sod;
        # es decir, si algún rol tiene incompatibilidades, lo trae en este df; luego habrá
        # que ver si esas incompatibilidades también las tiene el usuario: ésta es la cond
        # por la cual deben aparecer los registros en el reporte final.
        # -----------------------------------------------------------
        # Le agregué el "left" para debugging, pero no debería ser necesario (porque lo que no matchea no lo debería necesitar porque no tiene incompatibilidades).
        # df_tmp1 = pd.merge(df_concat, df_sod, how="left", on=["APPL_NAME", "ROLE_NAME"])  # por cada respo/rol trae todas sus incompatibilidades

        # df_tmp1 = pd.merge(left=df_concat, how="inner", right=df_sod, on=["APPL_NAME", "ROLE_NAME"], suffixes=["_con", "_sod"])  # por cada respo/rol trae todas sus incompatibilidades
        df_tmp1 = pd.merge(left=df_concat, right=df_sod, how="inner",
                           left_on=["APPL_NAME", "MODULE_NAME", "ROLE_NAME"],
                           right_on=["APPL_NAME", "MODULE_NAME", "ROLE_NAME"],
                           suffixes=["_usr", "_sod"])
        print(" "); print("df_tmp1 (df_concat + df_sod) = "); print(df_tmp1)
        df_tmp1 = df_tmp1.sort_values(["EMAIL_ADDRESS", "APPL_NAME"])
        if self.with_debug:
            # f.quick_excel(df_tmp1, "Rep_Proceso1", extension='csv')  # esto es lentísimo y crea un archivo de 160 Mb de 1.3 millones de líneas.
            f.quick_excel(df_tmp1, "Rep_Proceso1", extension='xlsx')

        # -----------------------------------------------------------
        # El df_tmp1 tiene los roles del usuario y las incompatibilidades
        # previstas para cada rol; el df_tmp3 es el cruce de esas incompatibilidades
        # con los roles asignados al usuario, para ver si tiene alguna de esas
        # incompabilidades. Si las tiene, va al reporte.
        # -----------------------------------------------------------

        # A la lista de roles y sus incompatibilidades, lo cruzo con la del usuario y sus respos: cada cruce debería representar 1 incompatibilidad.
        # df_tmp3 = pd.merge(left=df_tmp1, right=df_concat, left_on=["EMAIL_ADDRESS", "APPL_INCOMP_NAME", "INCOMP_ROLE_NAME"],
        #                    right_on=["EMAIL_ADDRESS", "APPL_NAME", "ROLE_NAME"], suffixes=["_inc", "_usr"])

        # esto es una prueba x icartes e ivan_derechinsky; confirmado está ok: con esto trae los cruces de Oracle con ATP1/ATP3 (el merge anterior no los trae).
        # df_tmp3 = pd.merge(left=df_tmp1, right=df_concat, how="inner",
        #                    left_on=["EMAIL_ADDRESS", "INCOMP_ROLE_NAME"],
        #                    right_on=["EMAIL_ADDRESS", "ROLE_NAME"], suffixes=["_inc", "_usr"])
        # print(" "); print("df_tmp3 (original) = "); print(df_tmp3)


        # esto es otra prueba: para jyamamoto cruza los roles con los módulos
        df_tmp3 = pd.merge(left=df_tmp1, right=df_concat, how="inner",
                           left_on=["EMAIL_ADDRESS", "APPL_INCOMP_NAME", "INCOMP_MODULE_NAME", "INCOMP_ROLE_NAME"],
                           right_on=["EMAIL_ADDRESS", "APPL_NAME", "MODULE_NAME", "ROLE_NAME"], suffixes=["_inc", "_usr"])
        print(" "); print("df_tmp3 (original) = "); print(df_tmp3.shape); print(df_tmp3)



        # df_tmp3 = df_tmp3[["EMAIL_ADDRESS", "USER_NAME_usr", "APPL_NAME_inc", "MODULE_NAME_con", "ROLE_NAME_inc", "ROLE_COUNTRY_inc", "APPL_INCOMP_NAME", "INCOMP_MODULE_NAME", "INCOMP_ROLE_NAME", "ROLE_COUNTRY_usr", "COMBINATION", "EMPLOYEE_COUNTRY_usr", "SUPERVISOR_usr", "SUPER_SUPERVISOR_usr"]]
        # df_tmp3 = df_tmp3[["EMAIL_ADDRESS", "USER_NAME_usr", "APPL_NAME_inc", "MODULE_NAME", "ROLE_NAME_inc", "ROLE_COUNTRY_inc", "APPL_INCOMP_NAME", "INCOMP_MODULE_NAME", "INCOMP_ROLE_NAME", "ROLE_COUNTRY_usr", "COMBINATION", "EMPLOYEE_COUNTRY_usr", "SUPERVISOR_usr", "SUPER_SUPERVISOR_usr"]]
        df_tmp3 = df_tmp3[["EMAIL_ADDRESS", "USER_NAME_usr", "APPL_NAME_inc", "MODULE_NAME_inc", "ROLE_NAME_inc", "ROLE_COUNTRY_inc", "APPL_INCOMP_NAME", "INCOMP_MODULE_NAME", "INCOMP_ROLE_NAME", "ROLE_COUNTRY_usr", "COMBINATION", "EMPLOYEE_COUNTRY_usr", "SUPERVISOR_usr", "SUPER_SUPERVISOR_usr"]]

        # df_tmp3 = df_tmp3.rename(columns={"USER_NAME_usr": "USER_NAME", "APPL_NAME_inc": "APPL_NAME", "MODULE_NAME_con": "MODULE_NAME", "ROLE_NAME_inc": "ROLE_NAME", "ROLE_COUNTRY_inc": "COUNTRY_RESP", "ROLE_COUNTRY_usr": "COUNTRY_INCOMP", "EMPLOYEE_COUNTRY_usr": "EMPLOYEE_COUNTRY", "SUPERVISOR_usr": "SUPERVISOR", "SUPER_SUPERVISOR_usr": "SUPER_SUPERVISOR"})
        df_tmp3 = df_tmp3.rename(columns={"USER_NAME_usr": "USER_NAME", "APPL_NAME_inc": "APPL_NAME", "MODULE_NAME_inc": "MODULE_NAME", "ROLE_NAME_inc": "ROLE_NAME", "ROLE_COUNTRY_inc": "COUNTRY_RESP", "ROLE_COUNTRY_usr": "COUNTRY_INCOMP", "EMPLOYEE_COUNTRY_usr": "EMPLOYEE_COUNTRY", "SUPERVISOR_usr": "SUPERVISOR", "SUPER_SUPERVISOR_usr": "SUPER_SUPERVISOR"})
        df_tmp3 = df_tmp3.sort_values(["EMAIL_ADDRESS", "ROLE_NAME"])

        print(" "); print("df_tmp3 (final) = "); print(df_tmp3)
        if self.with_debug:
            f.quick_excel(df_tmp3, "Rep_Proceso3")
        logging.info("- Proceso incompatibilidades")

        # -----------------------------------------------------------
        logging.info("+ Limpio duplicados")
        # -----------------------------------------------------------
        print("  Cantidad de registros antes de remover duplicados: ", len(df_tmp3))

        # Con esto borro las combinaciones donde ["Rol_1", "Rol_2"] == ["Rol_2", "Rol_1"] -

        cols = ['ROLE_NAME','INCOMP_ROLE_NAME'] # OJO! esto funciona perfecto!
        #cols = ['INCOMP_ROLE_NAME','ROLE_NAME']  # esto es una prueba (y quedó bien!! sinó, enroca el module_name con el incomp_role_name y los pone al revés).

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
        logging.info("- Limpio duplicados")

        # -----------------------------------------------------------
        logging.info("+ Verifico falsos positivos")
        # -----------------------------------------------------------
        # df_final = df_fin[df_fin["COUNTRY_RESP"] == df_fin["COUNTRY_INCOMP"]]  # con esta condición no cruza ATP1 con ATP3
        # df_final = df_fin[(df_fin["APPL_NAME"] == df_fin["APPL_INCOMP_NAME"]) & (df_fin["COUNTRY_RESP"] == df_fin["COUNTRY_INCOMP"])]

        # Saco esto porque se está comiendo 1 registro de ecantonio, donde es Oracle vs Clerk (ATP1), y no tiene country_incomp.
        # df_final = df_fin[((df_fin["APPL_NAME"] == 'ORACLE') & (df_fin["APPL_INCOMP_NAME"] == 'ORACLE') & (df_fin["COUNTRY_RESP"] == df_fin["COUNTRY_INCOMP"])) |
        #                   (df_fin["APPL_NAME"] == 'ATP1') | (df_fin["APPL_NAME"] == 'ATP3')]

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
        logging.info("- Verifico falsos positivos")

        # -----------------------------------------------------------
        logging.info("+ Recupero los Supervisores")
        # -----------------------------------------------------------
        df_csv = c.getCSV()
        df_final = pd.merge(left=df_final, right=df_csv, on="EMAIL_ADDRESS", how="left")
        print(" "); print("df_final (con supervisores) = "); print(df_final)

        # -----------------------------------------------------------
        logging.info("+ Genero el reporte final")
        # -----------------------------------------------------------
#        df_final = df_final[["EMAIL_ADDRESS", "USER_NAME", "APPL_NAME", "MODULE_NAME", "ROLE_NAME", "APPL_INCOMP_NAME", "INCOMP_MODULE_NAME", "INCOMP_ROLE_NAME", "COMBINATION", "SUPERVISOR", "SUPER_SUPERVISOR", "EMPLOYEE_COUNTRY"]]
        df_final = df_final[["EMAIL_ADDRESS", "USER_NAME", "APPL_NAME", "MODULE_NAME", "ROLE_NAME", "APPL_INCOMP_NAME", "INCOMP_MODULE_NAME", "INCOMP_ROLE_NAME",
                              "COMBINATION", "FULL_NAME", "SUPERVISOR_NAME", "SUPERVISOR_EMAIL_ADDRESS", "SUPER_SUPERV_NAME", "SUPER_SUPERV_EMAIL", "COUNTRY_CODE"]]
        df_final.sort_values(["EMAIL_ADDRESS","APPL_NAME","ROLE_NAME"], inplace=True)
        df_final.reset_index(drop=True, inplace=True)
        f.quick_excel(df_final, "Reporte_SOD_Compliance")
        logging.info("- Genero el reporte final")


if __name__ == "__main__":
    print("=======/ START /=======")
    start = datetime.now()
    # app = QApplication(sys.argv)
    myapp = Main()
    # myapp.show()
    # app.exec_()
    myapp.process()
    print("Processing elapsed time (report_main): {}".format(datetime.now() - start))
    print("=======/ END /=======")
