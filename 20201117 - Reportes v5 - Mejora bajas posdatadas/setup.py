from PyQt5 import uic
from PyQt5.QtWidgets import QDialog, QMessageBox, QFileDialog
from os import path
from configparser import ConfigParser
import funcs as f

class Setup(QDialog):
    def __init__(self):
        print("+ Setup")
        QDialog.__init__(self)
        uic.loadUi("screens/scr_setup.ui", self)
        self.linORA_Host.setFocus()
        self.btnFind.clicked.connect(self.findCSV)
        self.btnFindDB.clicked.connect(self.findDB)
        self.btnOK.clicked.connect(self.parSetup)
        self.btnCancel.clicked.connect(self.close_ui)

        # Cuando abro la pantalla debo cargar el contenido del archivo en pantalla.
        self.loadSetup()

        print("- Setup")

    def loadSetup(self):
        # Si el archivo de configuración no existe, lo crea.
        if path.exists("params/users.conf") is False:
            ok = QMessageBox.Yes

            if ok == QMessageBox.Yes:
                # Crea el archivo users.conf con todas las secciones, pero vacío (sin datos).
                print("+ Configura")
                print("+ Parser")
                cparser = ConfigParser()  # crea el objeto ConfigParser
                print("- Parser")

                print("+ Section AD")
                cparser.add_section("AD")
                cparser.set("AD", "Server", self.linAD_Server.text())
                cparser.set("AD", "Port", self.linAD_Port.text())
                cparser.set("AD", "User Name", self.linAD_UserName.text())
                cparser.set("AD", "Password", self.linAD_Pwd.text())
                cparser.set("AD", "Search Base", self.linAD_SearchBase.text())
                print("- Section AD")

                print("+ Section Oracle")
                cparser.add_section("ORACLE")
                cparser.set("ORACLE", "Host", self.linORA_Host.text())
                cparser.set("ORACLE", "SID", self.linORA_SID.text())
                cparser.set("ORACLE", "User Name", self.linORA_UserName.text())
                cparser.set("ORACLE", "Password", self.linORA_Pwd.text())
                print("- Section Oracle")

                print("+ Section Opecus")
                cparser.add_section("OPECUS")
                cparser.set("OPECUS", "Host", self.linOPQ_Host.text())
                cparser.set("OPECUS", "Port", self.linOPQ_Port.text())
                cparser.set("OPECUS", "Database", self.linOPQ_Database.text())
                cparser.set("OPECUS", "User Name", self.linOPQ_UserName.text())
                cparser.set("OPECUS", "Password", self.linOPQ_Pwd.text())
                print("+ Section Opecus")

                print("+ Section RRHH")
                cparser.add_section("RRHH")
                cparser.set("RRHH", "File Location", self.linCSV_FileLocation.text())
                print("- Section RRHH")

                print("+ Section eMail")
                cparser.add_section("EMAIL")
                cparser.set("EMAIL", "From", self.txtEmailFrom.text())
                cparser.set("EMAIL", "Password", self.txtEmailPwd.text())
                cparser.set("EMAIL", "To", self.txtEmailTo.text())
                print("- Section RRHH")

                print("+ Section LOGIN")
                cparser.add_section("LOGIN")
                cparser.set("LOGIN", "UserName", "")
                print("- Section LOGIN")

                print("+ Section DB")
                cparser.add_section("DB")
                cparser.set("DB", "File Location", "")
                print("- Section DB")

                print("+ SFTP")
                cparser.add_section("SFTP")
                cparser.set("SFTP", "Host", "")
                cparser.set("SFTP", "Port", "")
                cparser.set("SFTP", "UserName", "")
                cparser.set("SFTP", "Pwd", "")
                cparser.set("SFTP", "HostKeys", "")
                cparser.set("SFTP", "RemotePath", "")
                cparser.set("SFTP", "Filename", "")
                print("- SFTP")

                print("+ Crea archivo")
                with open("params/users.conf", "w") as archivo:
                    cparser.write(archivo)
                print("- Crea archivo")
                print("- Configura")
        else:
            print("+ Recupera")

            # Si el archivo existe lo lee y carga las variables en pantalla.
            print("+ Parser & read")
            cparser = ConfigParser()
            cparser.read("params/users.conf")
            print("- Parser & read")

            print("AD")
            self.linAD_Server.setText(cparser.get("AD", "Server"))
            self.linAD_Port.setText(cparser.get("AD", "Port"))
            self.linAD_UserName.setText(cparser.get("AD", "User Name"))
            self.linAD_Pwd.setText(f.dec(cparser.get("AD", "Password")))
            self.linAD_SearchBase.setText(cparser.get("AD", "Search Base"))

            print("ORACLE")
            self.linORA_Host.setText(cparser.get("ORACLE", "Host"))
            self.linORA_SID.setText(cparser.get("ORACLE", "SID"))
            self.linORA_UserName.setText(cparser.get("ORACLE", "User Name"))
            self.linORA_Pwd.setText(f.dec(cparser.get("ORACLE", "Password")))

            print("OPECUS")
            self.linOPQ_Host.setText(cparser.get("OPECUS", "Host"))
            self.linOPQ_Port.setText(cparser.get("OPECUS", "Port"))
            self.linOPQ_Database.setText(cparser.get("OPECUS", "Database"))
            self.linOPQ_UserName.setText(cparser.get("OPECUS", "User Name"))
            self.linOPQ_Pwd.setText(f.dec(cparser.get("OPECUS", "Password")))

            print("RRHH")
            self.linCSV_FileLocation.setText(cparser.get("RRHH", "File Location"))

            print("EMAIL")
            self.txtEmailFrom.setText(cparser.get("EMAIL", "From"))
            self.txtEmailPwd.setText(f.dec(cparser.get("EMAIL", "Password")))
            self.txtEmailTo.setText(cparser.get("EMAIL", "To"))

            print("RRHH")
            self.linDB_FileLocation.setText(cparser.get("DB", "File Location"))

            print("SFTP")
            self.txtSFTP_Host.setText(cparser.get("SFTP", "Host"))
            self.txtSFTP_Port.setText(cparser.get("SFTP", "Port"))
            self.txtSFTP_UserName.setText(cparser.get("SFTP", "UserName"))
            self.txtSFTP_Pwd.setText(f.dec(cparser.get("SFTP", "Pwd")))
            self.txtSFTP_HostKeys.setText(cparser.get("SFTP", "HostKeys"))
            self.txtSFTP_RemotePath.setText(cparser.get("SFTP", "RemotePath"))
            self.txtSFTP_Filename.setText(cparser.get("SFTP", "Filename"))

            print("- Recupera")

    def parSetup(self):
        print("+ Setea users.conf")
        print("+ Parser")
        cparser = ConfigParser()  # crea el objeto ConfigParser
        cparser.read("params/users.conf")
        print("- Parser")

        print("+ AD")
        cparser.set("AD", "Server", self.linAD_Server.text())
        cparser.set("AD", "Port", self.linAD_Port.text())
        cparser.set("AD", "User Name", self.linAD_UserName.text())
        cparser.set("AD", "Password", f.enc(self.linAD_Pwd.text()))
        cparser.set("AD", "Search Base", self.linAD_SearchBase.text())
        print("- AD")

        print("+ Oracle")
        cparser.set("ORACLE", "Host", self.linORA_Host.text())
        cparser.set("ORACLE", "SID", self.linORA_SID.text())
        cparser.set("ORACLE", "User Name", self.linORA_UserName.text())
        cparser.set("ORACLE", "Password", f.enc(self.linORA_Pwd.text()))
        print("- Oracle")

        print("+ Opecus")
        cparser.set("OPECUS", "Host", self.linOPQ_Host.text())
        cparser.set("OPECUS", "Port", self.linOPQ_Port.text())
        cparser.set("OPECUS", "Database", self.linOPQ_Database.text())
        cparser.set("OPECUS", "User Name", self.linOPQ_UserName.text())
        cparser.set("OPECUS", "Password", f.enc(self.linOPQ_Pwd.text()))
        print("- Opecus")

        print("+ RRHH")
        cparser.set("RRHH", "File Location", self.linCSV_FileLocation.text())
        print("- RRHH")

        print("+ EMAIL")
        cparser.set("EMAIL", "From", self.txtEmailFrom.text())
        cparser.set("EMAIL", "Password", f.enc(self.txtEmailPwd.text()))
        cparser.set("EMAIL", "To", self.txtEmailTo.text())
        print("- EMAIL")

        print("+ DB")
        cparser.set("DB", "File Location", self.linDB_FileLocation.text())
        print("- DB")

        print("+ SFTP")
        cparser.set("SFTP", "Host", self.txtSFTP_Host.text())
        cparser.set("SFTP", "Port", self.txtSFTP_Port.text())
        cparser.set("SFTP", "UserName", self.txtSFTP_UserName.text())
        cparser.set("SFTP", "Pwd", f.enc(self.txtSFTP_Pwd.text()))
        cparser.set("SFTP", "HostKeys", self.txtSFTP_HostKeys.text())
        cparser.set("SFTP", "RemotePath", self.txtSFTP_RemotePath.text())
        cparser.set("SFTP", "Filename", self.txtSFTP_Filename.text())
        print("- SFTP")

        print("+ Crea archivo")
        with open("params/users.conf", "w") as archivo:
            cparser.write(archivo)
        print("- Crea archivo")

        # Cierro la ventana y finalizo.
        print("- Setea users.conf")
        self.close()

    def findCSV(self):
        options = QFileDialog.Options()
        fileName, _ = QFileDialog.getOpenFileName(self, "QFileDialog.getOpenFileName()", "",
                                                  "CSV Files (*.csv);;Text Files (*.txt);;All Files (*)",
                                                  options=options)
        if fileName:
            print(fileName)
            self.linCSV_FileLocation.setText(fileName)
        else:
            self.linCSV_FileLocation.setText(None)

    def findDB(self):
        options = QFileDialog.Options()
        fileName, _ = QFileDialog.getOpenFileName(self, "QFileDialog.getOpenFileName()", "",
                                                  "DB Files (*.db);;All Files (*)",
                                                  options=options)
        if fileName:
            print(fileName)
            self.linDB_FileLocation.setText(fileName)
        else:
            self.linDB_FileLocation.setText(None)

    def close_ui(self):
        print("+ Cierra Setup")
        self.close()
        print("- Cierra Setup")
