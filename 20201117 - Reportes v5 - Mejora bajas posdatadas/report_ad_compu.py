import pandas as pd
import threading
import datetime
import time
import funcs as f
import connections as c

#pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

# ----------------------------------------------
# AD - Computadoras
# ----------------------------------------------
def reportADComputers(self, envia_sftp=False):
    print("+ MiHilo")
    hilo = MiHilo(args=(envia_sftp, ), daemon=False)
    hilo.start()
    print("- MiHilo")


class MiHilo(threading.Thread):
    def __init__(self, group=None, target=None, name=None, args=(), kwargs=None, *, daemon=None):
        print("+ __init__ de MiHilo")
        super().__init__(group=group, target=target, name=name, daemon=daemon)
        print(args)
        self.envia_sftp = args[0]   # envía al sftp
        print("- __init__ de MiHilo")

        self.w = f.wemerge()
        self.w.show()
        print("- __init__ de MiHilo")


    def run(self):
        print("+ RUN")
        dt_start = datetime.datetime.now()
        print("run: reportType = ", self.reportType)
        print("run: rtype = ", self.rtype)

        # ----------------------------------------------------------------------------
        # AD
        # ----------------------------------------------------------------------------
        ad = c.getAD()
        df_ad = ad.get_computers()

        if self.reportType == "Completo":
            df_inactivos = ad.get_inactives()
            # Concateno los activos con los inactivos.
            df_ad = pd.concat([df_ad, df_inactivos])
            filename = "Reporte_AD_Inactividad_(Full)"
        else:
            filename = "Reporte_AD_Inactividad"

        print(" "); print("df_ad (AD):"); print(df_ad)
        f.quick_excel(df_ad, filename, final=True, put_sftp=self.envia_sftp)

        self.w.wclose()

        print("- RUN")
        print("Processing elapsed time {}".format(datetime.datetime.now() - dt_start))  # esto muestra el tiempo de ejecución.