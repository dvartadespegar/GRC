# =================================================================================================================================
# Module Name: Main.py
# Purpose    : Consultar los datos principales de usuarios y emitir reportes de control.
# Parameters :
# Notes      :
# History    :
# 17/06/2020 - DVartabedian : Original version
# 23/11/2020 - DVartabedian : Elegir cada reporte con checkbox, tener 1 solo proceso de  ejecución de reportes; control
#                             de ejecución con logs; eliminación de los threadings y ejecución en cascada, para para el
#                             schedule de los reportes. La versión anterior tenía 2 problemas: al ser multithreading la
#                             cantidad de conexiones por base se podía excder de 5 y al subir el archivo al SFTP a veces
#                             ocurría que el archivo no se terminaba de generar y intentaba subir antes de que existiera
#                             el archivo en el directorio; luego, no subía.
# 14/04/2021 - DVartabedian : Se modifican las condiciones de los comentarios en los reportes para contemplar los casos
#                             de fechas de baja posdatadas: son usuarios activos.
#
# =================================================================================================================================
import sys
from PyQt5.QtWidgets import QApplication, QMainWindow, QMessageBox
from PyQt5 import uic, QtWidgets
import os
import platform
import logging
from datetime import datetime
import threading

import funcs as f
import setup as s
import get_users as gu
import scheduler as sch
import report_main as rmain
# import login as l


# ------------------------------------------------------------
# Configuro el logging: DEBUG, INFO, WARNING, ERROR, CRITICAL.
# ------------------------------------------------------------
#logging.basicConfig(level=logging.INFO,
#                    format='%(asctime)s %(threadName)-10s %(levelname)-8s %(name)-12s : %(message)s',
#                    datefmt='%d/%m %H:%M')



class Window(QMainWindow):
    def __init__(self):
        print("+ INIT")

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
        logging.info("=======/ START: " + self.start.strftime("%d/%m/%Y %H:%M:%S") + " /=======")
        print("- Configura logging")


        print("+ Abre pantalla")
        QMainWindow.__init__(self)
        uic.loadUi("screens/scr_usuarios.ui", self)
        print("- Abre pantalla")

        # --------------------------------------------------------------
        # Acciones de la solapa Users
        # --------------------------------------------------------------
        self.btnBuscar.clicked.connect(self.get_users)  # Busca Usuarios (1er tab)

        # --------------------------------------------------------------
        # Acciones de la Toolbar
        # --------------------------------------------------------------
        self.actUsers.triggered.connect(self.goTab_ActiveUsers)                       # User Details
        # self.actScheduler.triggered.connect(self.abreScheduler)                     # Scheduler
        self.actSetup.triggered.connect(self.abreSetup)                               # Setup
        self.actRepControl.triggered.connect(self.goTab_Reports)                      # Abre Reportes de Control

        self.btnPopSFTP.clicked.connect(self.call_popSFTP)                              # Pop SFTP
        self.btnCleanSFTP.clicked.connect(self.call_CleanSFTP)                          # Clean SFTP

        # --------------------------------------------------------------
        # Recibe el estado de cada checkbox (de cada reporte)
        # --------------------------------------------------------------
        print("+ Toma los estados de los checkbox de los reportes")
        # Oracle
        self.chkRepNomvsOracle.stateChanged.connect(self.changeState)
        self.chkRepOraDBUsers.stateChanged.connect(self.changeState)
        self.chkRepOraApplUsers.stateChanged.connect(self.changeState)
        self.chkRepOraInactivity.stateChanged.connect(self.changeState)
        self.chkRepOraSuperUser.stateChanged.connect(self.changeState)
        self.chkRepOraConfigurador.stateChanged.connect(self.changeState)
        self.chkRepOraUsersCreation.stateChanged.connect(self.changeState)
        self.chkRepOraRespFST.stateChanged.connect(self.changeState)
        # Nómina
        self.chkRepNominavsAD.stateChanged.connect(self.changeState)
        self.chkRepNominavsVPN.stateChanged.connect(self.changeState)
        self.chkRepNominavsGSuite.stateChanged.connect(self.changeState)
        self.chkRepNominavsSurgeMail.stateChanged.connect(self.changeState)
        self.chkRepNominavsATP1.stateChanged.connect(self.changeState)
        self.chkRepNominavsATP1PCI.stateChanged.connect(self.changeState)
        self.chkRepNominavsATP3.stateChanged.connect(self.changeState)
        self.chkRepNominavsATP3PCI.stateChanged.connect(self.changeState)
        self.chkRepNominavsSOX.stateChanged.connect(self.changeState)
        self.chkRepNominavsPS.stateChanged.connect(self.changeState)
        # Varios
        self.chkRepADCompleto.stateChanged.connect(self.changeState)
        self.chkRepADInactivity.stateChanged.connect(self.changeState)
        self.chkRepNomina.stateChanged.connect(self.changeState)
        print("- Toma los estados de los checkbox de los reportes")

        # ==============================================================
        # Genera todos los reportes
        # ==============================================================
        self.btnGenerarReportes.clicked.connect(self.call_GeneraReportes)

        # --------------------------------------------------------------
        # Debug
        # --------------------------------------------------------------
        self.chkDebug.stateChanged.connect(self.withDebug)  # with Debug

        # ------------------------------------------------------------------------------------------
        # Guardo el estado inicial del checkbox en el borg para usarlo después en f.quick_excel()
        # ------------------------------------------------------------------------------------------
        #borg = f.Borg()
        #borg.with_debug = self.chkDebug.isChecked()

        # --------------------------------------------------------------
        # Si el archivo de setup no existe, lo crea.
        # --------------------------------------------------------------
        if os.path.exists("params/users.conf") is False:
            ok = QMessageBox.question(self, "Setup", "El archivo de setup no existe; desea hacer la configuración ahora?",
                                      QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes)
            if ok == QMessageBox.Yes:
                self.abreSetup()
            else:
                quit()

        # --------------------------
        # Abro la pantalla de Login.
        # --------------------------
        #        self.login = l.Login()
        #        self.login.show()

        # --------------------------------------------------------
        # Muestro los reportes pendientes de subir al SFTP-GRC.
        # --------------------------------------------------------
        self.txtSFTPPending.setText(f.getPendings())

        print("- INIT")

    def changeState(self):  # OJO! NO borrar este método.
        pass

    def withDebug(self):
        borg = f.Borg()
        borg.with_debug = self.chkDebug.isChecked()
        print(borg.with_debug)

    # ------------------------------------------------------------------------------------------
    # Procedimientos de la Toolbar
    # ------------------------------------------------------------------------------------------
    def goTab_ActiveUsers(self):
        QtWidgets.QTabWidget.setCurrentIndex(self.tabWidget, 1)  # Voy al tabUsers
        self.txteMail.setFocus()

    def goTab_Reports(self):
        QtWidgets.QTabWidget.setCurrentIndex(self.tabWidget, 0)  # Voy al tabReports
        # self.txteMail.setFocus()

    def abreSetup(self):
        print("+ abreSetup")
        self.setup = s.Setup()
        self.setup.show()
        print("- abreSetup")

    def abreScheduler(self):
        print("+ abreScheduler")
        self.sch = sch.Scheduler()
        self.sch.show()
        print("- abreScheduler")

    # ------------------------------------------------------------------------------------------
    # Procedimientos del tab "Active Users"
    # ------------------------------------------------------------------------------------------
    def abreUsers(self):
        # Campos de ingreso de parámetros de búsqueda
        QtWidgets.QTabWidget.setCurrentIndex(self.tabWidget, 0)  # Voy al tabUsers
        self.txteMail.setFocus()

    def get_users(self):
        gu.buscar(self.txteMail, self.txtName, self.tblAD, self.tblOracle, self.tblCSV, self.tblOpecus, self.tblVPN, self.tblSurge, self.tblGMail)

    def call_SFTP(self):
        print("+ get_SFTP")
        l_rrhh = f.getSftp(remote_path="/upload", filename="RRHH.csv")
        l_surge = f.getSftp(remote_path="/upload/surge", filename="nwauth.txt")
        l_sox = f.getSftp(remote_path="/upload", filename="Reportes aplicaciones SOX.xlsx")

        if l_rrhh == "OK" and l_surge == 'OK' and l_sox == 'OK':
            ok = QMessageBox.information(self, "SFTP", "Los archivos fueron refrescados exitosamente", QMessageBox.Ok, QMessageBox.Ok)
            if l_rrhh != 'OK':
                ok = QMessageBox.information(self, "SFTP",
                                             "ERROR al recuperar el archivo RRHH.csv del SFTP. \nPor favor verifique que el archivo de destino no se encuentre en uso",
                                             QMessageBox.Ok, QMessageBox.Ok)
            if l_surge != 'OK':
                ok = QMessageBox.information(self, "SFTP", "ERROR al recuperar el archivo de SurgeMail del SFTP.", QMessageBox.Ok, QMessageBox.Ok)
            if l_sox != 'OK':
                ok = QMessageBox.information(self, "SFTP", "ERROR al recuperar el archivo de SOX del SFTP.", QMessageBox.Ok, QMessageBox.Ok)
        print("- get_SFTP")

    def upload_SFTP(self):
        print("+ upload_SFTP")
        l_ret = f.putSftp()
        if l_ret == "OK":
            ok = QMessageBox.information(self, "SFTP", "El archivo fue subido exitosamente", QMessageBox.Ok, QMessageBox.Ok)
        else:
            ok = QMessageBox.information(self, "SFTP",
                                         "ERROR al subir el archivo .xlsx al SFTP. \nPor favor verifique que el archivo de origen no se encuentre en uso",
                                         QMessageBox.Ok, QMessageBox.Ok)
        print("- upload_SFTP")

    def call_popSFTP(self):
        print("+ Pop_SFTP")
        l_pend_ant = self.txtSFTPPending.text()
        l_pendings = f.popQueue()
        self.txtSFTPPending.setText(l_pendings)
        if self.txtSFTPPending.text() >= l_pend_ant:
            ok = QMessageBox.information(self, "SFTP", "Hubo un error al subir los archivos al SFTP-GRC - Intente nuevamente", QMessageBox.Ok,
                                         QMessageBox.Ok)
        elif self.txtSFTPPending.text() == '0':
            ok = QMessageBox.information(self, "SFTP", "Todos los archivos subieron correctamente al SFTP-GRC - No hay pendientes", QMessageBox.Ok,
                                         QMessageBox.Ok)
        else:
            ok = QMessageBox.information(self, "SFTP", "Todavía quedan archivos pendientes - Intente nuevamente", QMessageBox.Ok, QMessageBox.Ok)

        print("- Pop_SFTP")

    def call_CleanSFTP(self):
        print("+ Clean_SFTP")
        l_pend_ant = self.txtSFTPPending.text()
        l_pendings = f.cleanQueue()
        self.txtSFTPPending.setText(l_pendings)
        if self.txtSFTPPending.text() >= l_pend_ant:
            ok = QMessageBox.information(self, "SFTP", "Hubo un error al limpiar los archivos pendientes de subir al SFTP-GRC - Intente nuevamente",
                                         QMessageBox.Ok, QMessageBox.Ok)
        elif self.txtSFTPPending.text() == '0':
            ok = QMessageBox.information(self, "SFTP", "Los archivos pendientes fueron eliminados - No hay pendientes", QMessageBox.Ok,
                                         QMessageBox.Ok)
        else:
            ok = QMessageBox.information(self, "SFTP", "Todavía quedan archivos pendientes - Intente nuevamente", QMessageBox.Ok, QMessageBox.Ok)

        print("- Clean_SFTP")

    """
    def activarSch(self):
        print("+ activarSch")
        # sch.activarSchedules(self)
        schedule = sch.Scheduler()
        schedule.activaSch()
        print("- activarSch")

    def desactivarSch(self):
        print("+ activarSch")
        sch.desactivarSchedules(self)
        print("- activarSch")
    """

    # ======================================================================
    #
    # Genera todos los reportes
    #
    # ======================================================================
    def call_GeneraReportes(self):
        start = datetime.now()
        print("+ Call Genera Reportes")

        # Cargo un diccionario con todos los reportes y su flag si fueron checked para saber si los tengo que ejecutar o no.
        # El 1er elemento es la key, y el 2° elemento es una lista con los dfs que se necesitarán en la ejecución de ese reporte.
        d_reports = {
            'chkRepNomvsOracle': [self.chkRepNomvsOracle.isChecked(), ['df_ora', 'df_nomina']],
            'chkRepOraDBUsers': [self.chkRepOraDBUsers.isChecked(), ['df_rrhh']],
            'chkRepOraApplUsers': [self.chkRepOraApplUsers.isChecked(), []],
            'chkRepOraInactivity': [self.chkRepOraInactivity.isChecked(), ['df_ora', 'df_nomina', 'df_vpn']],
            'chkRepOraSuperUser': [self.chkRepOraSuperUser.isChecked(), []],
            'chkRepOraConfigurador': [self.chkRepOraConfigurador.isChecked(), []],
            'chkRepOraUsersCreation': [self.chkRepOraUsersCreation.isChecked(), []],
            'chkRepOraRespFST': [self.chkRepOraRespFST.isChecked(), []],
            'chkRepNominavsAD': [self.chkRepNominavsAD.isChecked(), ['df_ad_actives', 'df_nomina']],
            'chkRepNominavsVPN': [self.chkRepNominavsVPN.isChecked(), ['df_vpn', 'df_nomina']],
            'chkRepNominavsGSuite': [self.chkRepNominavsGSuite.isChecked(), ['df_gsuite', 'df_nomina']],
            'chkRepNominavsSurgeMail': [self.chkRepNominavsSurgeMail.isChecked(), ['df_surge', 'df_nomina']],
            'chkRepNominavsATP1': [self.chkRepNominavsATP1.isChecked(), ['df_nomina']],
            'chkRepNominavsATP1PCI': [self.chkRepNominavsATP1PCI.isChecked(), ['df_nomina']],
            'chkRepNominavsATP3': [self.chkRepNominavsATP3.isChecked(), ['df_nomina']],
            'chkRepNominavsATP3PCI': [self.chkRepNominavsATP3PCI.isChecked(), ['df_nomina']],
            'chkRepNominavsSOX': [self.chkRepNominavsSOX.isChecked(), ['df_nomina', 'df_sox']],
            'chkRepNominavsPS': [self.chkRepNominavsPS.isChecked(), ['df_nomina', 'df_ps']],
            'chkRepADCompleto': [self.chkRepADCompleto.isChecked(), ['df_vpn', 'df_gsuite', 'df_surge']],
            'chkRepNomina': [self.chkRepNomina.isChecked(), ['df_nomina']],
            'chkRepADInactivity': [self.chkRepADInactivity.isChecked(), ['df_vpn', 'df_gsuite', 'df_surge']],
        }
        print(" "); print("d_reports = "); print(d_reports)

        # ----------------------------------------------------------------------------------------------------------
        # Crea una lista con valores únicos de los df del diccionario, según los reportes que se hayan seleccionado.
        # ----------------------------------------------------------------------------------------------------------
        print("+ Creo lista de los df comunes")
        """
        k = 0
        df_list = []
        for i, row in enumerate(d_reports.values()):
            if row[0]:
                k += 1
                for item in row[1]:
                    if item not in df_list:
                        df_list.append(item)
        """
        # Tomo los reportes que fueron seleccionados.
        d_checked_rep_list = list(filter(lambda x: x[1] if x[0] else None, d_reports.values()))
        # Tomo el 2° componente de cada lista (de los reportes ya seleccionados) y armo una nueva lista.
        df_list = []
        [(df_list := df_list + x[1]) for x in d_checked_rep_list]
        # Elimino los duplicados.
        df_list = set(df_list)
        print("df_list (lista de los df comunes): ", df_list)


        print("- Creo lista de los df comunes")

        if len(df_list) == 0:
            ok = QMessageBox.question(self, "Reportes", "No se ha seleccionado ningún reporte", QMessageBox.Yes, QMessageBox.Yes)
            return

        # -----------------------------------------------------------
        # Bajo los archivos necesarios del SFTP.
        # -----------------------------------------------------------
        if "df_vpn" in df_list:
            ok = QMessageBox.question(self, "Setup", "Ya ejecutó la actualización del reporte de VPN?", QMessageBox.Yes | QMessageBox.No, QMessageBox.Yes)
            if ok == QMessageBox.No:
                return
            l_vpn = f.getSftp(remote_path="/upload", filename="VPN.csv")
            if l_vpn != 'OK':
                ok = QMessageBox.information(self, "VPN", "ERROR al recuperar el archivo VPN.csv del SFTP.", QMessageBox.Ok, QMessageBox.Ok)
                return
        if "df_rrhh" in df_list or "df_nomina" in df_list:
            l_rrhh = f.getSftp(remote_path="/upload", filename="RRHH.csv")
            if l_rrhh != 'OK':
                ok = QMessageBox.information(self, "RRHH", "ERROR al recuperar el archivo RRHH.csv del SFTP. \nPor favor verifique que el archivo de destino no se encuentre en uso",
                                             QMessageBox.Ok, QMessageBox.Ok)
                return
        if "df_surge" in df_list:
            l_surge = f.getSftp(remote_path="/upload/surge", filename="nwauth.txt")
            if l_surge != 'OK':
                ok = QMessageBox.information(self, "SFTP", "ERROR al recuperar el archivo de SurgeMail del SFTP.", QMessageBox.Ok, QMessageBox.Ok)
                return
        if "df_sox" in df_list:
            l_sox = f.getSftp(remote_path="/upload", filename="Reportes aplicaciones SOX.xlsx")
            if l_sox != 'OK':
                ok = QMessageBox.information(self, "SFTP", "ERROR al recuperar el archivo de SOX del SFTP.", QMessageBox.Ok, QMessageBox.Ok)
                return
        if "df_ps" in df_list:
            l_ps = f.getSftp(remote_path="/upload", filename="Usuarios PS.xls")
            if l_ps != 'OK':
                logging.info("ERROR al recuperar el archivo de Usuarios PS.xls del SFTP.", QMessageBox.Ok, QMessageBox.Ok)
                return


        # ===========================================================
        # Genero los reportes seleccionados.
        # ===========================================================
        l_full = self.chkCompleto.isChecked()
        l_sftp = self.chkSendSFTP.isChecked()
        rmain.reports_main(self, completo=l_full, envia_sftp=l_sftp, reports=d_reports, rep_list=df_list, inact_days=self.spnInactiveDays.value())

        # try:
        #     print("threading.active_count() = {}".format(threading.active_count()))
        #     print("threading.enumerate() = {}".format(threading.enumerate()))
        # except Exception as err:
        #     print("ERROR al mostrar el estado del threading; ", err)

        """
        # Espero hasta que terminen todos los reportes: con esto no finaliza con error, pero tampoco muestra el avance del progress_bar.
        while threading.active_count() > 1:
            # print("threading.active_count() = {}".format(threading.active_count()))
            pass
        """

        # self.txtSFTPPending.setText(f.getPendings())

        # Restaura los checkbox.
        print("+ Restaura los checkbox")
        self.chkOracle.setChecked(False)
        self.chkRepNomvsOracle.setChecked(False)
        self.chkRepOraDBUsers.setChecked(False)
        self.chkRepOraApplUsers.setChecked(False)
        self.chkRepOraInactivity.setChecked(False)
        self.chkRepOraSuperUser.setChecked(False)
        self.chkRepOraConfigurador.setChecked(False)
        self.chkRepOraDBUsers.setChecked(False)
        self.chkRepOraUsersCreation.setChecked(False)
        self.chkRepOraRespFST.setChecked(False)
        self.chkNomina.setChecked(False)
        self.chkRepNominavsAD.setChecked(False)
        self.chkRepNominavsVPN.setChecked(False)
        self.chkRepNominavsGSuite.setChecked(False)
        self.chkRepNominavsSurgeMail.setChecked(False)
        self.chkRepNominavsATP1.setChecked(False)
        self.chkRepNominavsATP1PCI.setChecked(False)
        self.chkRepNominavsATP3.setChecked(False)
        self.chkRepNominavsATP3PCI.setChecked(False)
        self.chkRepNominavsSOX.setChecked(False)
        self.chkRepNominavsPS.setChecked(False)
        self.chkRepADCompleto.setChecked(False)
        self.chkRepNomina.setChecked(False)
        self.chkRepADInactivity.setChecked(False)
        print("- Restaura los checkbox")

        print("Main Processing elapsed time {}".format(datetime.now() - start))
        print("- Call Genera Reportes")


app = QApplication(sys.argv)
window = Window()
# window.showMaximized()
window.show()
app.exec_()
