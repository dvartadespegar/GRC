# =============================================================================
# Name...: Proceso de asignación de drives según G#
# Purpose: Busca quién es el G# de cada legajo.
# Params.: Como input se toma el archivo RRHH.csv que se baja del SFTP de RRHH.
# Output.: Genera un archivo RRHH_G#.xlsx con los datos del G# de cada legajo
#          en las columnas G_ASSIGN_EMAIL_ADDRESS y G_ASSIGN.
# Notes..: La columna G# informa si el legajo de esa fila es un G#.
# History: 2021/06/21  DVartabedian  Initial version.
#
# =============================================================================
import pandas as pd
import numpy as np
from datetime import datetime
import funcs as f
import logging
import platform
import os


class Main:
    def __init__(self):
        self.start = datetime.now()
        self.rrhh_g_filename = None     # Este valor lo voy a llenar después con el nombre del archivo generado por quick_excel (archivo RRHH_G_<fecha_hora>.xlsx final).

        print("+ Configura logging")
        if platform.platform().startswith('Windows'):
            self.log_filename = os.path.join(os.getcwd(), 'log_' + datetime.now().strftime("%Y%m%d_%H%M%S") + ".log")
        else:
            self.log_filename = os.path.join(os.getenv('HOME'), 'log_' + datetime.now().strftime("%Y%m%d_%H%M%S") + ".log")
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s %(threadName)-10s %(levelname)-8s %(name)-27s : %(message)s',
                            datefmt='%d/%m %H:%M:%S',
                            filename=self.log_filename,
                            filemode="w")
        print("- Configura logging")

        logging.info("=======/ START: " + self.start.strftime("%d/%m/%Y %H:%M:%S") + " /=======")

        #pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', None)

        print("+ Habilito el debug")
        self.debug = f.Debug()
        self.debug.enable_debug(debug=True, level=1)
        print("- Habilito el debug")

    def process(self):
        start = datetime.now()

        # -----------------------------------------------------------
        logging.info("+ Traigo el archivo RRHH.csv del SFTP")
        # -----------------------------------------------------------
        l_ok = f.get_sftp(remote_path="/upload", filename="RRHH.csv")
        if l_ok != 'OK':
            logging.error("ERROR al recuperar el archivo RRHH.csv del SFTP")
            return
        logging.info("- Traigo el archivo RRHH.csv del SFTP")

        # -----------------------------------------------------------
        logging.info("+ Genero el df de RRHH.csv")
        # -----------------------------------------------------------
        df = f.get_csv()
        logging.info("- Genero el df de RRHH.csv")

        # -----------------------------------------------------------
        logging.info("+ Genero la columna G#")
        # -----------------------------------------------------------
        cond = [ # G1
                 ((df["JOB_FAMILY"].str.upper() == "MANDOS MEDIOS") & ((df["PUESTO"].str.upper().str.contains("SR")) |
                                                                       (df["PUESTO"].str.upper().str.contains("SENIOR")) |
                                                                       (df["PUESTO"].str.upper().str.contains("MANAGER")))),
                 # G2
                 ((df["JOB_FAMILY"].str.upper() == "GERENTES") & (df["PUESTO"].str.upper().str.contains("DIRECTOR"))),
                 # G3
                 ((df["JOB_FAMILY"].str.upper() == "GERENTES") & (df["PUESTO"].str.upper().str.contains("VP"))),
               ]
        result = ["G1", "G2", "G3"]
        df["G#"] = np.select(cond, result, default=None)
        logging.info("- Genero la columna G#")

        # Reseteo el índice del df
        df.reset_index(drop=True, inplace=True)

        # -----------------------------------------------------------
        logging.info("+ Genero las columnas G_ASSIGN que informan quién es el G# de un legajo")
        # -----------------------------------------------------------
        df["G_ASSIGN_EMAIL_ADDRESS"] = None
        df["G_ASSIGN"] = None
        for i in range(0, len(df.index)):
            m = df.loc[i, "EMAIL_ADDRESS"]
            ret = self.find_g(df_g=df, p_email=m)
#            if len(ret) == 0:  # pd.isna(ret):
#                df.loc[i, "G_ASSIGN_EMAIL_ADDRESS"] = df.loc[i, "G_ASSIGN"] = None
#            else:
            df.loc[i, "G_ASSIGN_EMAIL_ADDRESS"] = ret[0]
            df.loc[i, "G_ASSIGN"] = ret[1]

        logging.info("- Genero las columnas G_ASSIGN que informan quién es el G# de un legajo")

        # -----------------------------------------------------------
        logging.info("+ Genero el archivo RRHH_G#")
        # -----------------------------------------------------------
        self.rrhh_g_filename = self.debug.quick_excel(df=df, title="RRHH_G#")
        logging.info("- Genero el archivo RRHH_G#")

    # --------------------------------------------------
    # Busco quién es el G# de un legajo
    # --------------------------------------------------
    def find_g(self, df_g=None, p_email=None):
        print("+ p_email = ", p_email)
        if pd.isna(p_email) or len(p_email) == 0:
            return [None, None]
        l_email = p_email.lower().strip()
        df_sup = df_g.loc[df_g["EMAIL_ADDRESS"] == l_email]
        l_sup_email = df_sup["MANAGER_EMAIL_ADDRESS"].to_list()[0]
        continua = True
        while continua:
            if pd.isna(l_sup_email):
                continua = False
            else:
                df_sup = df_g.loc[df_g["EMAIL_ADDRESS"].str.lower() == l_sup_email]
                print("  -", l_sup_email, "==>", df_sup["G#"].to_list()[0])
                if len(df_sup["MANAGER_EMAIL_ADDRESS"]) > 0 and pd.isna(df_sup["TERMINATION_DATE"].to_list()[0]) and not pd.isna(df_sup["G#"].to_list()[0]):
                    continua = False
                else:
                    l_sup_email = df_sup["MANAGER_EMAIL_ADDRESS"].to_list()[0]
        l_sup_email = None if l_sup_email == "damian.scokin@despegar.com" else l_sup_email
        return [l_sup_email, df_sup["G#"].to_list()[0]]

    def end(self):
        # -----------------------------------------------------------
        logging.info("+ Envío el archivo + log por mail")
        # -----------------------------------------------------------
        t = open('distribution_list.txt', 'r')
        l_to = t.read().split()  # el split deja cada fila del archivo como un elemento en una lista.
        if len(l_to) == 0:
            logging.info("No hay datos en la lista de distribución")
        else:
            logging.info("  Distribution List: " + str(l_to))
            print("  Distribution List: ", l_to)
            l_subject = "Proceso asignación de drives según G#"
            l_body = "Este mail fue enviado automáticamente, por favor no responder."
            l_filename = [self.log_filename, self.rrhh_g_filename]
            # Envío el mail
            f.send_mail(p_to=l_to, p_subject=l_subject, p_body=l_body, p_filename=l_filename)
        logging.info("- Envío el archivo + log por mail")

        # -----------------------------------------------------------
        print("Fin del proceso")
        # -----------------------------------------------------------
        logging.info("Processing elapsed time: {}".format(datetime.now() - self.start))
        logging.info("=======/ END: " + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + " /=======")


if __name__ == "__main__":
    myapp = Main()
    myapp.process()
    myapp.end()
