import pandas as pd
import sqlite3 as db
import base64
import pysftp
import openpyxl
import os
from datetime import datetime
import yagmail as yg


# --------------------------------------------
# Busca parámetros en la base users.db
# --------------------------------------------
def get_params(key):
    local_from = os.getcwd().replace("\\", "/")
    l_file_location = local_from + "/params/users.db"
    print("  l_file_location =", l_file_location)
    conn = db.connect(l_file_location)
    df = pd.read_sql("Select * From Params Where Key = '" + key + "' Order by Id", conn)
    conn.close()
    return df


# --------------------------------------------
# Baja el archivo de SurgeMail del SFTP
# --------------------------------------------
def get_sftp(remote_path, filename):
    print("+ get_Sftp")
    print("  filename = ", filename)

    print("+ Recupera parámetros")
    df_params = get_params(key="SFTP_RRHH")

    # Creo las variables dinámicamente.
    for i in range(0, len(df_params.index)):
        globals()[str(df_params.loc[i, "Variable_Name"]).lower()] = df_params.loc[i, "Value"]
    print("- Recupera parámetros")

    l_filename = filename                       # lo tomo del parámetro.
    sftp_from = remote_path+"/"+l_filename
    sftp_to = l_filename
    print("- Recupera parámetros")

    print("+ CnOpts")
    cnopts = pysftp.CnOpts()
    print("- CnOpts")

    if cnopts.hostkeys.lookup(l_host) == None:
        hostkeys = cnopts.hostkeys
        cnopts.hostkeys = None

    with pysftp.Connection(host=l_host, username=l_user, password=base64.b64decode(l_pwd).decode("utf-8"), cnopts=cnopts) as sftp:
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
        print("  sftp_from = ", sftp_from)
        print("  sftp_to = ", sftp_to)
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

    print("- get_Sftp")
    return "ERROR"


# --------------------------------------------
# Genera el df del archivo RRHH.csv
# --------------------------------------------
def get_csv():
    print("+ Connect CSV")
    l_path = os.getcwd().replace("\\", "/")
    print("  l_path =", l_path)

    csv = pd.read_csv(l_path + "/RRHH.csv")
    df_csv = pd.DataFrame(csv)

    # Elimino los legajos duplicados.
    df_csv.sort_values(['EMAIL_ADDRESS','TERMINATION_DATE'], ascending=[True, True], na_position='first', inplace=True)
    df_csv.drop_duplicates(subset="EMAIL_ADDRESS", keep='first', inplace=True)

    # Genero la columna "FULL_NAME" y dejo el mail en lowercase.
    df_csv["FULL_NAME"] = df_csv["APELLIDO"] + ", " + df_csv["PRIMER_NOMBRE"] + df_csv["SEGUNDO_NOMBRE"].apply(lambda x: " "+x if x is None else "")
    df_csv["EMAIL_ADDRESS"] = df_csv["EMAIL_ADDRESS"].str.lower()
    df_csv["MANAGER_EMAIL_ADDRESS"] = df_csv["MANAGER_EMAIL_ADDRESS"].str.lower()

    print("- Connect CSV")
    return df_csv


# --------------------------------------------
# Send_Mail
# --------------------------------------------
def send_mail(p_to=None, p_subject=None, p_body=None, p_filename=None):
    print("+ Send_Mail")
    print("+ Recupera parámetros")
    df_params = get_params(key="SEND_MAIL")

    print("+ Recupera parámetros")
    for i in range(0, len(df_params.index)):
        globals()[str(df_params.loc[i, "Variable_Name"]).lower()] = df_params.loc[i, "Value"]
    print("- Recupera parámetros")

    print("+ Envía el mail")
    try:
        yag = yg.SMTP(l_from, base64.b64decode(l_pwd).decode("utf-8"))
        yag.send(to=p_to, subject=p_subject, contents=p_body, attachments=p_filename)
    except yg.error as err:
        print(err)
    print("+ Envía el mail")
    print("- Send_Mail")


# -------------------------------------------------------------------
# Debugging
# - Level:
#   - 1: Debugging básico, sólo procesos
#   - 2: Incluye algo de detalle, con visualización de df's
#   - 3: Incluye todo el detalle, con visualización y Reportes de df's.
# -------------------------------------------------------------------
class Debug():
    def __init__(self):
        self.l_repnum = 0
        self.l_debug = False

    def enable_debug(self, debug=False, level=0):
        self.l_debug = debug
        self.l_level = level

    def print_debug(self, df, print_title=None, print_report=None):
        if self.l_debug:
            if print_title is not None:
                print(" "); print(print_title + ":"); print(df)
            if print_report is not None and self.l_level >= 3:
                self.l_repnum = self.l_repnum + 1
                tit = "Rep_" + str(self.l_repnum).zfill(2) + "_" + print_report
                self.quick_excel(df=df, title=tit)

    def quick_excel(self, df, title, extension="xlsx"):
        filename = title + "_" + datetime.now().strftime("%Y%m%d_%H%M%S")
        if extension == "xlsx":
            filename = filename + ".xlsx"
            writer = pd.ExcelWriter(filename)

            # df.to_excel(writer, sheet_name=title, index=False)
            GROUP_LENGTH = 500000
            for i in range(0, len(df), GROUP_LENGTH):
                df.iloc[i:i + GROUP_LENGTH, ].to_excel(writer, sheet_name='Row {}'.format(i), index=False)

            # Le damos a cada columna el ancho correspondiente
            for sheet in writer.sheets:
                try:
                    # worksheet = writer.sheets[title]
                    worksheet = writer.sheets[sheet]
                    for i, col in enumerate(df):
                        try:
                            max_width = df[col].astype(str).str.len().max()  # ancho de cada celda (excepto header)
                            max_width = max(max_width, len(col)) + 2  # len(col) : es el ancho de cada header
                        except:
                            # max_width = max_width + 2
                            max_width = len(col) + 2

                        worksheet.set_column(i, i, max_width)
                except Exception as err:
                    print("ERROR al calcular el ancho de cada columna; ", err)

            writer.save()
            print(" "); print("Se generó el archivo: ", filename)
            #writer.close()
            return filename
        elif extension == 'csv':
            filename = filename + ".csv"
            df.to_csv(filename, index=False, header=True, sep=";")
            return filename

