from datetime import datetime
import pandas as pd
import sqlite3 as db
import os
import cx_Oracle
import base64
import numpy as np


def quick_excel(df, title, extension="xlsx"):
    print("+ quick_excel")
    filename = title + "_" + datetime.now().strftime("%Y%m%d_%H%M%S")
    if extension == "xlsx":
        filename = filename + ".xlsx"
        print("  filename =", filename)
        writer = pd.ExcelWriter(filename, engine='xlsxwriter')  #, na_rep='NaN'

        # Si la longitud del df > 500.000, generará tantas solapas de 500.000 hasta completar la longitud del df (si tiene 700.000, generará 2 solapas).
        GROUP_LENGTH = 500000
        for i in range(0, len(df), GROUP_LENGTH):
            df.iloc[i:i + GROUP_LENGTH, ].to_excel(writer, sheet_name='Page_{}'.format(i), index=False)

        # Le damos a cada columna el ancho correspondiente
        for sheet in writer.sheets:
            try:
                worksheet = writer.sheets[sheet]

                for i, col in enumerate(df):
                    try:
#                        max_width = (df[col].astype(str).map(len).max(), len(col))
                        max_width = df[col].astype(str).str.len().max()   # ancho de cada celda (excepto header)
                        max_width = max(max_width, len(col)) + 2          # len(col) : ancho de cada header
                    except:
                        # max_width = max_width + 2
                        max_width = len(col) + 2
                    worksheet.set_column(i, i, max_width)

            except Exception as err:
                print("ERROR al calcular el ancho de cada columna; ", err)  # si sale el error "'Worksheet' object has no attribute 'set_column'" es porque no está instalado el paquete xlsxwriter.

        writer.save()
        print("  Archivo creado exitosamente!")
        # writer.close()
    elif extension == 'csv':
        filename = filename + ".csv"
        print("  filename =", filename)
        df.to_csv(filename, index=False, header=True, sep=";")
    print("- quick_excel")


def dec(pwd):
    dec_s = base64.b64decode(pwd).decode("utf-8")    # decrypt
    return dec_s


def get_params(key):
    local_from = os.getcwd().replace("\\", "/")
    l_file_location = local_from + "/users.db"
    conn = db.connect(l_file_location)
    df = pd.read_sql("Select * From Params Where Key = '" + key + "' Order by Id", conn)
    conn.close()
    return df


def formateaFecha(p_value, p_format_in, p_format_out):
    try:
        l_date = datetime.strptime(p_value, p_format_in)   # toma la fecha como string y la transforma a date.
        l_date = l_date.strftime(p_format_out)             # toma la fecha date y le da formato dd/mm/yyyy (queda como string).
    except:
        l_date = None
    return l_date
