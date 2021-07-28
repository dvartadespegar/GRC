import base64
import datetime
import time
import os
from configparser import ConfigParser
import pandas as pd
from PyQt5.QtWidgets import QTableWidgetItem, QDialog, QMessageBox
from PyQt5 import QtWidgets, QtCore, QtGui
# import pygsheets
#from tabulate import tabulate
import openpyxl             # si bien no se usa directamente, al usar el quick_excel se invoca internamente.
import pysftp
import sqlite3 as db
import logging
# OJO! Se requiere también la instalación de: pip install xlsxwriter
# (esto se usa en el quick_excel para que no tire el error 'Worksheet' object has no attribute 'set_column')
# se requiere la instalación de: pip install xlrd

#pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


def getADStatusCode(p_value):
    st = str(p_value)
    d_values = {'1': "Script", '2': "DISABLE", '8': 'HomeDir_Required', '16': 'LOCKOUT', '32': 'Passwd_NotReqd',
                '64': 'Passwd_Cant_Change', '128': 'Encrypted_Text_Pwd_Allowed', '256': 'Temp_Duplicate_Account',
                '512': 'Normal_Account', '514': 'DISABLE ACCOUNT', '544': 'Enabled, Password Not Required',
                '546': 'DISABLE, PASSW NOT REQUIRED', '2048': 'Interdomain_Trust_Account',
                '2080': 'Interdomain Trust Control - Passw Not Required',
                '4096': 'Workstation_Trust_Account', '8192': 'Server_Trust_Account', '65536': 'Dont_Expire_Password',
                '66048': 'Enabled, Password Doesn’t Expire', '66050': 'DISABLED, Password Doesn’t Expire',
                '66080': 'Enabled, Password Doesn’t Expire & Not Required',
                '66082': 'DISABLED, Password Doesn’t Expire & Not Required', '131072': 'Mns_Logon_Account',
                '262144': 'SmartCard_Required', '262656': 'Enabled, Smartcard Required',
                '262658': 'DISABLED, Smartcard Required', '262690': 'DISABLED, Smartcard Required, Password Not Required',
                '328192': 'Enabled, Smartcard Required, Password Doesn’t Expire',
                '328194': 'DISABLED, Smartcard Required, Password Doesn’t Expire',
                '328224': 'Enabled, Smartcard Required, Password Doesn’t Expire & Not Required',
                '328226': 'DISABLED, Smartcard Required, Password Doesn’t Expire & Not Required',
                '524288': 'Truste_For_Delegation', '532480': 'Domain controller', '1048576': 'Not Delegated',
                '2097152': 'Use_Des_Key_Only', '4194304': 'Dont_Req_PreAuth', '8388608': 'PASSWORD_EXPIRED',
                '16777216': 'Trusted_To_Auth_For_Delegation', '67108864': 'Partial_Secrets_Account'}
    try:
        l_return = d_values[st]
        return l_return
    except:
        return "Valor " + "Nulo" if p_value is None else "Valor " + str(p_value) + " desconocido"


def enc(pwd):
    enc_b = base64.b64encode(pwd.encode("utf-8"))   # encrypt
    enc_s = str(enc_b.decode())                     # change bytes to string
    return enc_s


def dec(pwd):
    dec_s = base64.b64decode(pwd).decode("utf-8")    # decrypt
    return dec_s


def formateaFecha(p_value, p_format_in, p_format_out):
    try:
        l_date = datetime.datetime.strptime(p_value, p_format_in)   # toma la fecha como string y la transforma a date.
        l_date = l_date.strftime(p_format_out)                      # toma la fecha date y le da formato dd/mm/yyyy (queda como string).
    except:
        l_date = None
    return l_date


class Borg:
    _shared_state = {}
    def __init__(self):
        self.__dict__ = self._shared_state


def downTable(self, formato, isChecked, title, gsheet_name):
    print("+ Entró en downTable")

    # Determino qué df voy a usar.
    borg = Borg()
    df = borg.df if isChecked == True else borg.df2

    print(formato)
    if formato == ".xlsx file":
        # Genero un xlsx
        print("+ Genero el xlsx")
        filename = title + "_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + ".xlsx"
        writer = pd.ExcelWriter(filename)
        df.to_excel(writer, sheet_name='Reporte', index=False)
        writer.save()
        ok = QMessageBox.question(self, "Excel", "Se generó el archivo " + filename + " en la carpeta " + os.getcwd(), QMessageBox.Ok)
        print("- Genero el xlsx")
    elif formato == ".csv file":
        # Genero un csv
        print("+ Genero el csv")
        filename = "Oracle candidatos a baja " + datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + ".csv"
        df.to_csv(filename)
        ok = QMessageBox.question(self, "CSV", "Se generó el archivo " + filename + " en la carpeta " + os.getcwd(), QMessageBox.Ok)
        print("- Genero el csv")
    # elif formato == 'GSheet file':
    #     # Genero planilla en GSheet
    #     try:
    #         try:
    #             gc = pygsheets.authorize(service_file='client_secret.json')
    #         except Exception as err:
    #             print("ERROR al autorizar; ", err)
    #
    #         # open the google spreadsheet
    #         try:
    #             print("Open - gsheet_name =", gsheet_name)
    #             sh = gc.open(gsheet_name)
    #         except Exception as err:
    #             print("  Crea la planilla ", gsheet_name, " - Acordate que si la planilla es nueva, hay que compartirla con el usuario 'prueba-gsheet@airy-coil-267813.iam.gserviceaccount.com'")
    #             sh = gc.create(gsheet_name)
    #
    #         # select the first sheet
    #         print("Abre el 1er sheet")
    #         wks = sh[0]
    #
    #         print("Borro el 1er Sheet")
    #         wks.clear()
    #         print("- Borro el 1er Sheet")
    #
    #         # update the first sheet with df, starting at cell B2.
    #         print("+ Update")
    #         wks.set_dataframe(df, (1, 1))
    #         ok = QMessageBox.question(self, "GSheets", "Se actualizó el archivo ", gsheet_name, " en GSheets", QMessageBox.Ok)
    #         print("- Update")
    #     except Exception as err:
    #         print("ERROR al generar la planilla en GSheet; ", err)
    elif formato == 'eMail':
        import yagmail

        print("+ Primero genero el xlsx")
        filename = title + "_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + ".xlsx"
        writer = pd.ExcelWriter(filename)
        df.to_excel(writer, sheet_name='Reporte', index=False)
        writer.save()
        print("- Primero genero el xlsx")

        print("+ Envío la planilla por mail")
        cparser = ConfigParser()  # crea el objeto ConfigParser
        cparser.read("users.conf")
        c_email_from = cparser.get("EMAIL", "From")
        c_email_pwd = dec(cparser.get("EMAIL", "Password"))
        c_email_to = cparser.get("EMAIL", "To")

        try:
            yag = yagmail.SMTP(c_email_from, c_email_pwd)
            yag.send(to=c_email_to,
                    #cc= , bcc= ,
                     subject=title,
                     contents=["Este mail fue enviado automáticamente.", "Por favor no lo responda.", "Gracias."],
                     attachments=filename,
                    )
            print("El mail fue enviado exitosamente a ", c_email_to)
            ok = QMessageBox.question(self, "eMail", "Se envió por mail el archivo " + filename, QMessageBox.Ok)
        except yagmail.error as err:
            print("Error al enviar el mail; ", err)
            ok = QMessageBox.warning(self, "eMail", "ERROR al enviar el mail; ", err, QMessageBox.Ok)

        print("- Envío la planilla por mail")

    print("- Entró en downDbUsers")


def send_mail(self, to, subject, body=[], filename=None):
    import yagmail

    cparser = ConfigParser()  # crea el objeto ConfigParser
    cparser.read("users.conf")
    c_email_from = cparser.get("EMAIL", "From")
    c_email_pwd = dec(cparser.get("EMAIL", "Password"))
    if body is None or len(body) == 0:
        body = ["Este mail fue enviado automáticamente.", "Por favor no lo responda.", "Gracias."]

    print("Mail:", to, subject, body, c_email_from, c_email_pwd)
    print(body[0])

    try:
        yag = yagmail.SMTP(c_email_from, c_email_pwd)
        yag.send(to=to,
                 # cc= , bcc= ,
                 subject=subject,
                 contents=body,
                 attachments=filename,
                 )
        print("El mail fue enviado exitosamente a ", to)
    except yagmail.error as err:
        print("Error al enviar el mail; ", err)
        ok = QMessageBox.warning(self, "eMail", "ERROR al enviar el mail; ", err, QMessageBox.Ok)


def quick_excel(dfqe, title, final=False, put_sftp=False):
    try:
        borg = Borg()
        l_with_debug = borg.with_debug
    except:
        l_with_debug = False

    if l_with_debug or final:
        filename = title + "_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + ".xlsx"
        writer = pd.ExcelWriter("output/"+filename)
        dfqe.to_excel(writer, sheet_name=title, index=False)

        # Le damos a cada columna el ancho correspondiente.
        try:
            worksheet = writer.sheets[title]
            for i, col in enumerate(dfqe):
                max_width = dfqe[col].astype(str).str.len().max()     # ancho de cada celda (excepto header)
                try:
                    max_width = max(max_width, len(col)) + 2        # len(col) : es el ancho de cada header
                except:
                    max_width = max_width + 2
                worksheet.set_column(i, i, max_width)
        except Exception as err:
            print("ERROR al calcular el ancho de cada columna; ", err)

        writer.save()

        print("  Se generó el archivo: ", filename)

        # --------------------------------------------------------
        # Lo subo al SFTP.
        # --------------------------------------------------------
        if final and put_sftp:
            # putSftp(filename)
            local_from = os.getcwd().replace("\\", "/") + "/output"
            pushQueue(local_from, filename)


def showTable(self, isChecked, qtable):
    print("+ showTable")

    borg = Borg()
    try:
        if isChecked == True:
            dftab = borg.df
            print("--> Muestra valores de df")
        else:
            dftab = borg.df2
            print("--> Muestra valores de df2")
        existe = 1
    except:
        print("No hay datos para exportar")
        existe = 0

    if existe == 1:
        # Si el sort está habilitado (lo puse como default), lo saca.
        if qtable.isSortingEnabled() == True:
            qtable.setSortingEnabled(False)

        # Para que deje de tirar warnings hay que usar el qRegisterMetaType, pero no sé de dónde sale ni cómo mandarle argumentos.
        #qRegisterMetaType(QList)

        qtable.setRowCount(0)

        print("  + Carga registros pantalla - Checked (muestra todo?) = ", isChecked)
        for i in range(0, len(dftab.index)):
            qtable.insertRow(i)
            for j in range(0, len(dftab.columns)):          # OJO que recorre las columnas del df, no del QtWidgetTable.
                try:
                    try:
                        l_type = dftab.iloc[i, j].dtype
                    except:
                        l_type = "str"

                    if l_type in ("int64", "float64"):
                        item = QTableWidgetItem()
                        try:
                            item.setData(QtCore.Qt.DisplayRole, int(dftab.iloc[i, j]))
                            qtable.setItem(i, j, item)
                        except:
                            qtable.setItem(i, j, 0)  # si dio error, lo clavo en cero.
                    else:
                        qtable.setItem(i, j, QTableWidgetItem(str(dftab.iloc[i, j])))
                except Exception as err:
                    print("ERROR en i = ", i, ", j = ", j, err)
        print("  - Carga registros en pantalla - Checked (muestra todo?) =", isChecked)

    # Vuelve a habilitar el sort.
    qtable.setSortingEnabled(True)

    # Resize to content.
    try:
        qtable.setSizeAdjustPolicy(QtWidgets.QAbstractScrollArea.AdjustToContents)
        qtable.resizeColumnsToContents()
    except Exception as err:
        print("ERROR al hacer el Resize to content;", err)
    print("- showTable")


class winbar(QDialog):
    def __init__(self):
        QtWidgets.QWidget.__init__(self)
        self.setWindowTitle('Progress')
        self.setGeometry(770, 370, 370, 57)
        self.setFixedSize(370, 57)

        self.progress = QtWidgets.QProgressBar(self)
        self.progress.setGeometry(30, 10, 337, 30)

        self.setWindowState(QtCore.Qt.WindowActive)
        self.setWindowFlag(QtCore.Qt.WindowCloseButtonHint, False)

        self.activateWindow()
        self.start = datetime.datetime.now()

        self.g_step = 100
        self.l_step = 1

    def setTitle(self, title):
        self.setWindowTitle(title)

    def max(self, valor):
        self.progress.setMaximum(valor)

    # def global_step(self, steps):
    #     self.g_step = int(70 / steps)
    #
    # def local_step(self, steps):
    #     self.l_step = int(self.g_step / steps)

    def adv(self, advance):
        try:
            # print("  advance = ", advance)
            # print("  maximum = ", self.progress.maximum())
            if advance > self.progress.maximum():
                advance = self.progress.maximum()
            self.progress.setValue(advance)
            self.progress.show()
        except Exception as err:
            print("ERROR en progress_bar; ", err)

    def cl(self):
        self.close()
        # print("Processing elapsed time {}".format(datetime.datetime.now() - self.start))


class wemerge(QDialog):
    def __init__(self):
        QtWidgets.QWidget.__init__(self)
        self.setGeometry(770, 370, 370, 57)
        self.setFixedSize(370, 57)

        self.label = QtWidgets.QLabel(self)
        self.label.setGeometry(30, 10, 337, 30)
        font = QtGui.QFont("Arial", 11)
        self.label.setFont(font)
        self.label.setAlignment((QtCore.Qt.AlignCenter))
        self.label.setText("Procesando...")

        self.setWindowState(QtCore.Qt.WindowActive)
        self.setWindowFlag(QtCore.Qt.WindowCloseButtonHint, False)
        self.start = datetime.datetime.now()

    def setTitle(self, title):
        self.setWindowTitle(title)

    def wclose(self):
        self.close()

def readConf():
    cparser = ConfigParser()
    cparser.read("params/users.conf")
    return cparser


def getSftp(remote_path, filename):
    print("+ getSftp")
    print("  filename = ", filename)
    print("+ Recupera parámetros")
    cp = readConf()
    l_local_dir = cp.get("RRHH", "File Location")
    hostkeys = cp.get("SFTP", "HostKeys")
    l_host = cp.get("SFTP", "Host")
    l_user = cp.get("SFTP", "Username")
    l_pwd = dec(cp.get("SFTP", "Pwd"))
    l_filename = filename                       # lo tomo del parámetro.
    sftp_from = remote_path+"/"+l_filename
    sftp_to = l_local_dir+"/"+l_filename
    print("  host = ", l_host)
    print("  filename = ", l_filename)
    print("  from = ", sftp_from)
    print("  to = ", sftp_to)
    print("- Recupera parámetros")

    print("+ CnOpts")
    cnopts = pysftp.CnOpts()
    print("- CnOpts")

    if cnopts.hostkeys.lookup(l_host) == None:
        hostkeys = cnopts.hostkeys
        cnopts.hostkeys = None

    with pysftp.Connection(host=l_host, username=l_user, password=l_pwd, cnopts=cnopts) as sftp:
        print("Se estableció la conexión exitosamente ... ")

        # Switch to a remote directory
        #sftp.cwd(l_remote_dir)
        #print("  Cambiamos a:", l_remote_dir)

        # Obtain structure of the remote directory '/var/www/vhosts'
        directory_structure = sftp.listdir_attr()

        # Print data
        for attr in directory_structure:
            print(" ", attr.filename, attr)

        print("+ getRemote")
        try:
            sftp.get(sftp_from, sftp_to)
        except Exception as err:
            print("ERROR al bajar el archivo del SFTP (posiblemente el archivo de destino se encuentre en uso); ", err)
            sftp.close()
            return "ERROR"

        sftp.close()
        print("- getRemote")

        print("- getSftp")
        return "OK"

    print("- getSftp")
    return "ERROR"



def putSftp(filename):
    print("+ Recupera parámetros")

    # cp = readConf()
    local_from = os.getcwd().replace("\\","/") + "/output" # +"/"+filename
    print("  local_from = ", local_from)
    l_file_location = local_from + "/params/users.db" #"DB", "File Location")
    print("  DB Localization: ", l_file_location)
    conn = db.connect(l_file_location)
    df = pd.read_sql("Select * From Params Where Key = 'SFTP_GRC' Order by Id", conn)

    # Creo las variables dinámicamente.
    for i in range(0, len(df.index)):
        # globals()[str(df.loc[i, "Variable_Name"]).lower()] = df.loc[i, "Value"]
        locals()[str(df.loc[i, "Variable_Name"]).lower()] = df.loc[i, "Value"]

    print("  host = ", l_host)
    print("  filename = ", filename)
    print("  from = ", local_from)
    sftp_to = filename #+ "_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + ".xlsx"
    print("  to = ", sftp_to)
    print("  l_remote_dir = ", l_remote_dir)
    print("  l_user = ", l_user)
    print("  l_hostkeys = ", l_hostkeys)
    hostkeys = l_hostkeys
    print("- Recupera parámetros")

    print("+ CnOpts")
    cnopts = pysftp.CnOpts()
    print("- CnOpts")

    print("+ HostKeys")
    if cnopts.hostkeys.lookup(l_host) == None:
        hostkeys = cnopts.hostkeys
        cnopts.hostkeys = None
    print("- HostKeys")

    # with pysftp.Connection(host=l_host, username=l_user, password=l_pwd, cnopts=cnopts) as sftp:
    try:
        sftp = pysftp.Connection(host=l_host, username=l_user, password=l_pwd, cnopts=cnopts)

        print("Se estableció la conexión exitosamente ... ")

        # Switch to a remote directory
        # sftp.cwd(l_remote_dir)
        # print("  Cambiamos a:", l_remote_dir)

        # Obtain structure of the remote directory '/var/www/vhosts'
        directory_structure = sftp.listdir_attr()

        # Muestra los archivos que se encuentran en el SFTP.
        # for attr in directory_structure:
        #     print(" ", attr.filename, attr)

        print("+ putRemote")
        try:
            sftp.put((local_from + "/" + filename), sftp_to)
            sftp.close()
            logging.info("SFTP: archivo "+filename+" subido exitosamente!")
            return "OK"
        except Exception as err:
            print("ERROR al subir el archivo al SFTP (posiblemente el archivo de origen se encuentre en uso); ", err)
            logging.error("ERROR al subir el archivo al SFTP (posiblemente el archivo de origen se encuentre en uso); "+ err)
            sftp.close()

            # ----------------------
            # lo agrega a la cola
            # ----------------------
            pushQueue(local_from, filename)

            return "ERROR"
        print("- putRemote")

    except Exception as err:
        print("ERROR al conectarse al SFTP; ", err)
        logging.error("ERROR al conectarse al SFTP; "+ str(err))

        # ----------------------
        # lo agrega a la cola
        # ----------------------
        pushQueue(local_from, filename)

        return "ERROR"



# ------------------------------------------------------
# Busca la fecha de última modificación de un archivo.
# ------------------------------------------------------
def getLastUpdateDate(path):
    try:
        ult_mod = os.path.getmtime(path)        # OJO! el path debe incluir el nombre del archivo: x ej: "C:/Users/daniel.vartabedian/RRHH/RRHH.csv"
        last_update_str = time.strftime("%d/%m/%Y %H:%M:%S", time.localtime(ult_mod))
        return last_update_str
    except Exception as err:
        print("ERROR al buscar el LastUpdateDate de un archivo")
        return "ERROR"



# -------------------------------------------------------------------
# Carga la cola de archivos pendientes de subir al SFTP
# -------------------------------------------------------------------
def pushQueue(path, filename):
    print("+ pushQueue")
    l_file_location = os.getcwd().replace("\\","/")+"/params/users.db"
    print("  DB Localization: ", l_file_location)
    conn = db.connect(l_file_location)

    cur = conn.cursor()
    sql = "Select ifnull(MAX(Id),0)+1 From SFTPQueue Where function = 'PUT';"
    print("  sql = ", sql)
    cur.execute(sql)
    l_id = cur.fetchone()[0]    # OJO! no se puede volver a llamar al fetchone porque avanza el puntero al próximo registro.
    print("  l_id =", l_id)

    sql = "INSERT INTO SFTPQueue (Id, Function, Path, Filename) VALUES ('"+str(l_id)+"', 'PUT', '"+path+"', '"+filename+"');"
    print("  sql =", sql)
    try:
        conn.execute(sql)
        conn.commit()
    except Exception as err:
        conn.rollback()
        print("ERROR al hacer el push; ", err)
    conn.close()
    print("- pushQueue")



# -------------------------------------------------------------------
# Descarga la cola de archivos pendientes de subir al SFTP
# -------------------------------------------------------------------
def popQueue():
    print("+ popQueue")
    l_file_location = os.getcwd().replace("\\", "/") + "/params/users.db"
    print("  DB Localization: ", l_file_location)
    conn = db.connect(l_file_location)


    # Creo las variables dinámicamente.
    df = pd.read_sql("Select * From Params Where Key = 'SFTP_GRC' Order by Id", conn)
    for i in range(0, len(df.index)):
        globals()[str(df.loc[i, "Variable_Name"]).lower()] = df.loc[i, "Value"]
    print("  l_host = ", l_host)
    print("  l_user = ", l_user)
    print("  l_remote_dir = ", l_remote_dir)

    """
    try:
        cnopts = pysftp.CnOpts(knownhosts='/params/known_hosts')
        cnopts.hostkeys = None
    except Exception as err:
        print("Error al buscar el CnOpts; ", err)
        try:
            print("+ CnOpts")
            cnopts = pysftp.CnOpts()
            print("- CnOpts")

            print("+ HostKeys")
            if cnopts.hostkeys.lookup(l_host) == None:
                hostkeys = cnopts.hostkeys
                cnopts.hostkeys = None
            print("- HostKeys")
        except Exception as err:
            print("ERROR al buscar el CnOpts (2); ", err)
    """

    # Nueva forma de conectarme...
    cnopts = pysftp.CnOpts()
    if cnopts.hostkeys.lookup(l_host) == None:
        hostkeys = cnopts.hostkeys
    cnopts.hostkeys = None


    # with pysftp.Connection(host=l_host, username=l_user, password=l_pwd, cnopts=cnopts) as sftp:
    try:
        sftp = pysftp.Connection(host=l_host, username=l_user, password=dec(l_pwd), cnopts=cnopts)

        print("Se estableció la conexión exitosamente ... ")

        # Switch to a remote directory
        # sftp.cwd(l_remote_dir)
        # print("  Cambiamos a:", l_remote_dir)

        # Obtain structure of the remote directory '/var/www/vhosts'
        directory_structure = sftp.listdir_attr()

        # Print data
        #for attr in directory_structure:
        #    print(" ", attr.filename, attr)

        print("+ putRemote")
        df = pd.read_sql("Select id, path, filename From SFTPQueue Where function = 'PUT' Order by Id;", conn)
        print(" "); print("df (archivos pendientes): "); print(df)

        for i in range(0, len(df.index)):
            try:
                local_dir = df.loc[i, "Path"] + "/" + df.loc[i, "Filename"]
                sftp.put(local_dir, df.loc[i, "Filename"])
                logging.info("SFTP: archivo "+str(df.loc[i, "Filename"])+" subido exitosamente!")

                sql = "DELETE FROM SFTPQueue WHERE Id = " + str(df.loc[i, "Id"]) + ";"
                print("  sql =", sql)
                try:
                    conn.execute(sql)
                    conn.commit()
                except Exception as err:
                    print("ERROR al hacer el Delete de la tabla SFTPQueue para el Id = ", df.loc[i, "Id"], "; ", err)
            except Exception as err:
                print("ERROR al subir el archivo al SFTP; ", err)
                logging.error("ERROR al subir el archivo al SFTP; "+ str(err))
                # Si el registro estaba en la tabla SFTPQueue, aunque no haya encontrado el archivo físico en el directorio, tengo que borrar el registro.
                sql = "DELETE FROM SFTPQueue WHERE Id = " + str(df.loc[i, "Id"]) + ";"
                print("  sql =", sql)
                try:
                    conn.execute(sql)
                    conn.commit()
                except Exception as err:
                    print("ERROR al hacer el Delete de la tabla SFTPQueue para el Id = ", df.loc[i, "Id"], "; ", err)

        conn.close()
        sftp.close()
        print("- putRemote")
        return getPendings()

    except Exception as err:
        print("ERROR al conectarse al SFTP; ", err)
        logging.error("ERROR al conectarse al SFTP; "+ str(err))
        conn.close()
        return getPendings()

    print("- popQueue")

def cleanQueue():
    print("+ cleanQueue")
    l_file_location = os.getcwd().replace("\\", "/") + "/params/users.db"
    print("  DB Localization: ", l_file_location)
    conn = db.connect(l_file_location)
    sql = "DELETE FROM SFTPQueue;"
    try:
        conn.execute(sql)
        conn.commit()
        return getPendings()
    except Exception as err:
        print("ERROR al hacer el Delete de la tabla SFTPQueue; ", err)
        return getPendings()


def getPendings():
    try:
        l_file_location = os.getcwd().replace("\\", "/") + "/params/users.db"
        print("  DB Localization: ", l_file_location)
        conn = db.connect(l_file_location)
        sql = "Select COUNT(1) From SFTPQueue Where function = 'PUT';"
        print("  sql = ", sql)
        cur = conn.cursor()
        cur.execute(sql)
        l_count = cur.fetchone()[0]  # OJO! no se puede volver a llamar al fetchone porque avanza el puntero al próximo registro.
        print("  l_count =", l_count)
        conn.close()
        return str(l_count)
    except:
        return "ERROR"

"""
class AdvProgBar():
    def __init__(self):
        self.g_step = 100
        self.l_step = 1

    def max(self, max):
        self.max = max

    def global_step(self, steps):
        self.g_step = int(70 / steps)

    def local_step(self, steps):
        self.l_step = int(self.g_step / steps)

    def adv_global(self):
"""
