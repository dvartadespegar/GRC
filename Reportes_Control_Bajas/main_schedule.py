from PyQt5.QtWidgets import QApplication, QMainWindow, QMessageBox
import sys
# import schedule
import datetime as dt
# import dateutil.relativedelta as dlt
# import time as tt
# import logging
import os, platform, logging
from datetime import datetime
import funcs as f
import report_main as rmain


# ------------------------------------------------------------
# Configuro el logging: DEBUG, INFO, WARNING, ERROR, CRITICAL.
# ------------------------------------------------------------
if platform.platform().startswith('Windows'):
    log_filename = os.path.join(os.getcwd(),'log_'+datetime.now().strftime("%Y%m%d_%H%M%S") + ".log")
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


        # --------------------------------------------------------
        # Ejecuto los reportes
        # --------------------------------------------------------
        # Estos procesos se ejecutan todos los miércoles a las 23.00 hs.
        # schedule.every().wednesday.at('23:00').do(self.get_files)
        # schedule.every().wednesday.at('23:15').do(rmain.reports_main, self, False, True, self.d_reports, self.df_list, 90)

        """
        # Proceso semanal
        schedule.every().wednesday.at("23:00").do(self.ejecuta_reportes, periodo="semanal")

        # Proceso mensual (se ejecuta a fin de mes)
        schedule.every().day.at("22:00").do(self.ejecuta_reportes, periodo="mensual")

        """

        """
        # Esto es para ejecutarlo inmediatamente.
        inicio1 = (start + dt.timedelta(minutes=1)).strftime("%H:%M")  # inicio de la bajada de archivos del sftp
        #inicio2 = (start + dt.timedelta(minutes=3)).strftime("%H:%M")  # inicio del procesamiento de los reportes
        print(" ")
        print("* La fecha/hora actual es: ", start.strftime("%d/%m/%Y %H:%M:%S"))
        #print("* Los download del SFTP comenzarán a ejecutarse a las: ", inicio1)
        #print("* El procesamiento de los reportes comenzará a ejecutarse a las: ", inicio2)
        print("* El procesamiento de los reportes comenzará a ejecutarse a las: ", inicio1)
        print(" ")
        
        schedule.every().day.at(inicio1).do(self.ejecuta_reportes, periodo="semanal")


        # ------------------------------------
        # Espera
        # ------------------------------------
        while True:
            schedule.run_pending()
            logging.info("En espera..."+format(dt.datetime.now(),"%d/%m/%Y %H:%M:%S"))
            tt.sleep(10) #(3600)                     # Verifica 1 vez por hora
        """

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


    # def ejecuta_reportes(self, periodo):
    def ejecuta_reportes(self):
        # finmes = dt.date.today() + dlt.relativedelta(day=31)
        # print("Fin Mes: ", finmes)
        # if (periodo == "mensual" and dt.date.today() == finmes) or (periodo == "semanal"):
        rmain.reports_main(self, completo=False, envia_sftp=True, reports=self.d_reports, rep_list=self.df_list, inact_days=90)


if __name__ == '__main__':
    logging.info("======= INICIO DEL PROCESO =======")
    start = dt.datetime.now()
    app = QApplication(sys.argv)
    window = Window()

    # Baja los archivos necesarios del SFTP
    window.get_files()

    # Ejecuta los reportes
    window.ejecuta_reportes()

    logging.info("Main Processing elapsed time {}".format(datetime.now() - start))
    logging.info("======== FIN DEL PROCESO =========")
    app.exec_()
