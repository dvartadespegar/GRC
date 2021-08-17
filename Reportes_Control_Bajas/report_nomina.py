import pandas as pd
import threading
import funcs as f
from datetime import datetime
import connections as c
from configparser import ConfigParser

#pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


def rep_nomina(self, envia_sftp=False, args=()):
    print(" "); print("+ REPORTE NOMINA")
    print("Params: envia_sftp =", envia_sftp)
    start = datetime.now()

    df_nomina = args[0]

#
#     print("+ MiHilo")
#     hilo = MiHilo(args=(envia_sftp, ), daemon=False)
#     print("- MiHilo")
#     hilo.start()
#
#
# class MiHilo(threading.Thread):
#     def __init__(self, group=None, target=None, name=None, args=(), kwargs=None, *, daemon=None):
#         print("+ __init__ de MiHilo")
#         super().__init__(group=group, target=target, name=name, daemon=daemon)
#         print(args)
#         self.envia_sftp = args[0]   # envía al sftp
#         print("- __init__ de MiHilo")
#
#         self.prog = f.winbar()
#         self.prog.setTitle("Reporte Nómina")
#         self.prog.show()
#
#     def run(self):
#         dt_start = datetime.now()
#         self.prog.adv(5)
#
#         df = c.nomina()
#         self.prog.adv(50)

    # ----------------------------------------------------------------------------------------------------------------
    # Dejo sólo las columnas que me interesan
    # ----------------------------------------------------------------------------------------------------------------
    df_nomina = df_nomina[["FULL_NAME", "EMAIL_ADDRESS", "END_DATE", "OPQ_USERNAME", "AD_SAMACCOUNTNAME", "AD_STATUS", "AD_LAST_LOGON_DATE", "ORIGEN"]]
    # self.prog.adv(90)

    f.quick_excel(df_nomina, "Reporte_Nomina", final=True, put_sftp=self.envia_sftp)

    print("Processing elapsed time {}".format(datetime.now() - start))
    print("- REPORTE NOMINA")
