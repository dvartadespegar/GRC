from PyQt5.QtWidgets import QApplication, QMainWindow, QMessageBox
import sys
import os, platform, logging
import datetime as dt
import dateutil.relativedelta as dlt
from datetime import datetime

import funcs as f
import report_main as rmain


# ------------------------------------------------------------
# Configuro el logging: DEBUG, INFO, WARNING, ERROR, CRITICAL.
# ------------------------------------------------------------
if platform.platform().startswith('Windows'):
    log_filename = os.path.join(os.getcwd()+"/output",'log_'+datetime.now().strftime("%Y%m%d_%H%M%S") + ".log")
else:
    log_filename = os.path.join(os.getenv('HOME'),'log_'+datetime.now().strftime("%Y%m%d_%H%M%S") + ".log")

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(threadName)-10s %(levelname)-8s %(name)-12s : %(message)s',
                    datefmt='%d/%m %H:%M:%S',
                    filename=log_filename,
                    filemode="w")


class Window(QMainWindow):
    def __init__(self):
        QMainWindow.__init__(self)
        logging.info("======= INICIO DEL PROCESO =======")
        start = dt.datetime.now()
        self.i = 0

        self.d_reports = {
            'chkRepNomvsOracle': [True, ['df_ora', 'df_nomina']],
            'chkRepOraDBUsers': [True, ['df_rrhh']],
            'chkRepOraApplUsers': [True, []],
            'chkRepOraInactivity': [True, ['df_ora', 'df_nomina', 'df_vpn']],
            'chkRepOraSuperUser': [True, []],
            'chkRepOraConfigurador': [True, []],
            'chkRepOraUsersCreation': [True, []],
            'chkRepOraRespFST': [True, []],
            'chkRepNominavsAD': [True, ['df_ad_actives', 'df_nomina']],
            'chkRepNominavsVPN': [True, ['df_vpn', 'df_nomina']],
            'chkRepNominavsGSuite': [True, ['df_gsuite', 'df_nomina']],
            'chkRepNominavsSurgeMail': [True, ['df_surge', 'df_nomina']],
            'chkRepNominavsATP1': [True, ['df_nomina']],
            'chkRepNominavsATP1PCI': [True, ['df_nomina']],
            'chkRepNominavsATP3': [True, ['df_nomina']],
            'chkRepNominavsSOX': [True, ['df_nomina', 'df_sox']],
            'chkRepNominavsPS': [True, ['df_nomina', 'df_ps']],
            'chkRepADCompleto': [False, ['df_vpn', 'df_gsuite', 'df_surge']],
            'chkRepNomina': [False, ['df_nomina']],
            'chkRepADInactivity': [True, ['df_vpn', 'df_gsuite', 'df_surge']],
        }
        # print(" "); print("d_reports = "); print(d_reports)

        # ----------------------------------------------------------------------------------------------------------
        # Crea una lista con valores únicos de los df del diccionario, según los reportes que se hayan seleccionado.
        # ----------------------------------------------------------------------------------------------------------
        print("+ Creo lista de los df comunes")
        self.df_list = []
        for i, row in enumerate(self.d_reports.values()):
            if row[0]:
                for item in row[1]:
                    if item not in self.df_list:
                        self.df_list.append(item)
        print("  df_list (lista de los df comunes): ", self.df_list)
        print("- Creo lista de los df comunes")

        # ----------------------------------------------------------------------------
        # PROCESAMIENTO DE LOS REPORTES
        # ----------------------------------------------------------------------------
        self.ejecuta_reportes(periodo="semanal")


    def get_files(self):
        # Traigo del SFTP los archivos que necesito para los reportes.
        l_rrhh = f.getSftp(remote_path="/upload", filename="RRHH.csv")
        if l_rrhh != 'OK':
            logging.info("ERROR al recuperar el archivo RRHH.csv del SFTP. \nPor favor verifique que el archivo de destino no se encuentre en uso")
            return
        l_surge = f.getSftp(remote_path="/upload/surge", filename="nwauth.txt")
        if l_surge != 'OK':
            logging.info("ERROR al recuperar el archivo de SurgeMail del SFTP.", QMessageBox.Ok, QMessageBox.Ok)
            return
        l_sox = f.getSftp(remote_path="/upload", filename="Reportes aplicaciones SOX.xlsx")
        if l_sox != 'OK':
            logging.info("ERROR al recuperar el archivo de SOX del SFTP.", QMessageBox.Ok, QMessageBox.Ok)
            return
        l_vpn = f.getSftp(remote_path="/upload", filename="VPN.csv")
        if l_vpn != 'OK':
            logging.info("ERROR al recuperar el archivo de VPN del SFTP.", QMessageBox.Ok, QMessageBox.Ok)
            return
        l_ps = f.getSftp(remote_path="/upload", filename="Usuarios PS.xls")
        if l_ps != 'OK':
            logging.info("ERROR al recuperar el archivo de Usuarios PS.xls del SFTP.", QMessageBox.Ok, QMessageBox.Ok)
            return

    def ejecuta_reportes(self, periodo):
        finmes = dt.date.today() + dlt.relativedelta(day=31)
        print("Fin Mes: ", finmes)
        if (periodo == "mensual" and dt.date.today() == finmes) or (periodo == "semanal"):
            self.get_files()
            print("+ Procesamiento Reportes")
            rmain.reports_main(self, completo=False, envia_sftp=True, reports=self.d_reports, rep_list=self.df_list, inact_days=90)
            print("- Procesamiento Reportes")


if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = Window()
    logging.info("=== FIN ===")
    app.exec_()
    exit()
