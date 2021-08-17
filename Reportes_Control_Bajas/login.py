from PyQt5 import uic, QtCore
from PyQt5.QtWidgets import QTableWidgetItem, QHeaderView, QDialog, QMessageBox
from configparser import ConfigParser
import datetime
import sqlite3 as s
import funcs as f
import connections as c

class Login(QDialog):
    def __init__(self):
        print("Entró al __init__ de Login")
        QDialog.__init__(self)
        uic.loadUi("screens/scr_login.ui", self)

        # ------------------------------------------------------------------
        print("Seteos de la clase")
        # ------------------------------------------------------------------
        self.txteMail.setFocus()
        self.setWindowFlag(QtCore.Qt.WindowCloseButtonHint, False)

        # Botones
        self.btnLogin.clicked.connect(self.proc_login)
        self.btnCancel.clicked.connect(self.proc_cancel)
        self.btnRegister.clicked.connect(self.proc_register)
        # Checkbox
        self.chkForgotPwd.stateChanged.connect(self.proc_forgotpwd)

        # ------------------------------------------------------------------
        print("+ Conecto con la base de datos")
        # ------------------------------------------------------------------
        try:
            self.conn = s.connect("users.db")
            self.cur = self.conn.cursor()
            tablas = ["""
                    CREATE TABLE IF NOT EXISTS Users (
                                                    User_Id         INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                                    User_Name       TEXT NOT NULL UNIQUE,
                                                    eMail           TEXT,
                                                    Pwd             TEXT,
                                                    Last_Logon_Date DATE
                                                    );
                    """
                    ]
            for tabla in tablas:
                self.cur.execute(tabla);
        except s.OperationalError as err:
                print("Error al abrir la base de datos; ", err)
        print("- Conecto con la base de datos")

        # --------------------------------------------------------------------------------------------------------------------------
        print("Si se activó el 'Keep me signed in', debe leer el archivo de configuración y poner las credenciales en pantalla")
        # --------------------------------------------------------------------------------------------------------------------------
        print("+ KeepSigned")
        self.cparser = ConfigParser()
        self.cparser.read("users.conf")
        self.txteMail.setText(self.cparser.get("LOGIN", "UserName"))
        print("Email recuperado del users.conf: ", self.txteMail.text())

        if self.txteMail.text() is None or len(self.txteMail.text()) == 0:
            pass
        else:
            l_search = self.txteMail.text().strip().lower()
            print("Busco en la BD al usuario: ", l_search)
            try:
                self.cur.execute("SELECT pwd FROM Users WHERE lower(eMail) = ?;", (l_search,))
            except Exception as err:
                print("ERROR al buscar en la BD; ", err)
            row = self.cur.fetchone()
            if row is None:
                pass
            else:
                self.txtPwd.setText(row[0])

            # ------------------------------------------------------------------
            # Marco el checkbox como checked.
            # ------------------------------------------------------------------
            self.chkKeepSigned.setChecked(True)

        print("- KeepSigned")


    def proc_login(self):
        # ------------------------------------------------------------------
        print("+ Valido eMail y Password")
        # ------------------------------------------------------------------
        try:
            if self.txteMail.text().strip() is None or len(self.txteMail.text().strip()) == 0:
                ok = QMessageBox.information(self, "eMail", "El eMail está vacío", QMessageBox.Ok, QMessageBox.Ok)
                self.txteMail.setFocus()
                return
            if self.txtPwd.text().strip() is None or len(self.txtPwd.text().strip()) == 0:
                ok = QMessageBox.information(self, "Password", "El Password está vacío", QMessageBox.Ok, QMessageBox.Ok)
                self.txtPwd.setFocus()
                return
        except Exception as err:
            print("ERROR al validar; ", err)
        print("- Valido eMail y Password")

        # ------------------------------------------------------------------
        print("+ Valido contra el AD si la cuenta está habilitada")
        # ------------------------------------------------------------------
        self.ad = c.getAD()
        try:
            data = self.ad.get_samAccountName(self.txteMail.text())
        except Exception as err:
            print("ERROR al buscar el mail en el AD; ", self.txteMail.text(), " --> ", err)

        if data[2].upper().find("DISABLED") > 0:
            ok = QMessageBox.warning(self, "AD", "Esta cuenta está deshabilitada", QMessageBox.Ok, QMessageBox.Ok)
            self.txtPwd.setFocus()
            return
        else:
            print("- Valido contra el AD si la cuenta está habilitada - Cuenta Habilitada")

        # ------------------------------------------------------------------
        print("+ Valido las credenciales de entrada contra mi base de datos")
        # ------------------------------------------------------------------
        l_search = self.txteMail.text().strip().lower()
        print("Busco en la BD al usuario: ", l_search)
        try:
            self.cur.execute("SELECT pwd FROM Users WHERE lower(eMail) = ?;", (l_search,))
        except Exception as err:
            print("ERROR al buscar en la BD; ", err)
        row = self.cur.fetchone()
        if row is None:
            ok = QMessageBox.information(self, "Login", "Usuario/Clave Inexistente", QMessageBox.Ok, QMessageBox.Ok)
            self.txteMail.setFocus()
            return
        else:
            print("Acceso Habilitado")

            # ------------------------------------------
            # Valido el Password.
            # ------------------------------------------
            if self.txtPwd.text() != row[0]:
                ok = QMessageBox.information(self, "Login", "Usuario/Clave Inexistente", QMessageBox.Ok, QMessageBox.Ok)
            else:
                self.conn.close()
                self.close()
        print("- Valido las credenciales de entrada contra mi base de datos")

        # ----------------------------------------------------------------------------------------------------
        # Si el checkbox "Keep me signed in" está activado, guardo el mail en el archivo de configuración.
        # ----------------------------------------------------------------------------------------------------
        l_eMail = self.txteMail.text().lower().strip()
        print("Setea con archivo users.conf para poder loguearse la próxima vez; ", l_eMail, self.chkKeepSigned.isChecked())
        try:
            if self.chkKeepSigned.isChecked():
                print("Seteó el MAIL del users.conf")
                self.cparser.set("LOGIN", "username", l_eMail)
            else:
                print("Limpió el LOGIN del users.conf")
                self.cparser.set("LOGIN", "username", "")

            with open("users.conf", "w") as archivo:
                self.cparser.write(archivo)
        except Exception as err:
            print("ERROR al guardar el mail en el archivo users.conf; ", err)


    def proc_cancel(self):
        self.conn.close()
        quit()


    def proc_register(self):
        # Valido eMail y Password.
        if self.txteMail.text().strip() is None or len(self.txteMail.text().strip()) == 0:
            ok = QMessageBox.information(self, "eMail", "El eMail está vacío", QMessageBox.Ok, QMessageBox.Ok)
            self.txteMail.setFocus()
            return
        if self.txtPwd.text().strip() is None or len(self.txtPwd.text().strip()) == 0:
            ok = QMessageBox.information(self, "Password", "El Password está vacío", QMessageBox.Ok, QMessageBox.Ok)
            self.txtPwd.setFocus()
            return

        # Valido contra el AD si la cuenta está habilitada.
        print("+ Valido contra el AD si la cuenta está habilitada")
        self.ad = c.getAD()
        try:
            data = self.ad.get_samAccountName(self.txteMail.text())
        except Exception as err:
            print("ERROR al buscar el mail en el AD; ", self.txteMail.text(), " --> ", err)

        if data[2].upper().find("DISABLED") > 0:
            ok = QMessageBox.warning(self, "AD", "Esta cuenta está deshabilitada", QMessageBox.Ok, QMessageBox.Ok)
            self.txtPwd.setFocus()
            return
        else:
            print("- Valido contra el AD si la cuenta está habilitada")

        # Verifico que no se registre un mail ya registrado.
        l_email = self.txteMail.text().strip().lower()
        print("Mail buscado: ", l_email)
        try:
            self.cur.execute("SELECT * FROM Users WHERE lower(eMail) = ?;", (l_email,))
        except Exception as err:
            print("ERROR al buscar en la BD; ", err)

        row = self.cur.fetchone()
        if row is None:     # Si no encuentra nada, está ok.
            print("Este mail no está registrado en la BD")
            pass
        else:
            ok = QMessageBox.information(self, "Login", "Usuario ya registrado", QMessageBox.Ok, QMessageBox.Ok)
            self.txteMail.setFocus()
            return

        # Registro las credenciales de acceso en la base de datos.
        try:
            sql = "INSERT INTO Users (User_Name, eMail, Pwd, Last_Logon_Date) VALUES (?, ?, ?, ?)"
            self.cur.execute(sql, [l_email, l_email, (self.txtPwd.text().strip()), (datetime.datetime.today()), ])
            self.conn.commit()
            ok = QMessageBox.information(self, "Login", "Usuario registrado exitosamente", QMessageBox.Ok, QMessageBox.Ok)

            # Llama al proc_login para que ingrese directamente con las credenciales que ya proporcionó.
            self.proc_login()

        except Exception as err:
            print("ERROR al Insertar en la BD; ", err)


    def proc_forgotpwd(self):
        if self.txteMail.text().strip() is None:
            ok = QMessageBox.information(self, "eMail", "Por favor complete su eMail", QMessageBox.Ok, QMessageBox.Ok)
            self.txteMail.setFocus()
            return

        self.cur.execute("SELECT pwd FROM Users WHERE lower(eMail) = ?;", (self.txteMail.text().strip().lower(),))
        row = self.cur.fetchone()
        if row is None:
            ok = QMessageBox.information(self, "Forgot Password", "Usuario no registrado", QMessageBox.Ok, QMessageBox.Ok)
            self.txteMail.setFocus()
            return
        else:
            pwd = ["Su clave es: ", row[0]]
            f.send_mail(self, to=(self.txteMail.text().strip()), subject="Password Recovery", body=pwd, filename=None)
            ok = QMessageBox.information(self, "Forgot Password", "Se envió un mail con su clave. Por favor checkee su casilla", QMessageBox.Ok, QMessageBox.Ok)

        self.proc_cancel()
