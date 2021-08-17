# import pandas as pd
import logging
import threading
from datetime import datetime
import sqlite3 as db
import funcs as f
import connections as c

import report_ended as re
import report_db as db
import report_inactivity as ri
import report_oracle_varios as rov
import report_nomina_vs_vpn as nvpn
import report_nomina_vs_ad as nad
import report_nomina_vs_gsuite as ngs
import report_ad as rad
# import report_appl_users as rau
import report_nomina_vs_surge as nsm
import report_nomina_vs_atp1 as natp1
import report_nomina_vs_atp1_pci as natp1pci
import report_nomina_vs_atp3 as natp3
import report_nomina_vs_atp3_pci as natp3pci
import report_nomina as nom
import report_nomina_vs_sox as nsox
import report_nomina_vs_ps as nps


def reports_main(self, completo=False, envia_sftp=False, reports={}, rep_list=[], inact_days=90):
    print("+ REPORTS MAIN")
    hilo = Thread_Reports_Main(args=(completo, envia_sftp, reports, rep_list, inact_days), daemon=False)
    hilo.start()
    # print("Threading isAlive?: ", hilo.is_alive())
    print("- REPORTS MAIN")

class Thread_Reports_Main(threading.Thread):
    def __init__(self, group=None, target=None, name=None, args=(), kwargs=None, *, daemon=None):
        print("+ MiHilo")
        super().__init__(group=group, target=target, name=name, daemon=daemon)
        # --- Parámetros ---------------------
        self.completo = args[0]
        self.envia_sftp = args[1]
        self.d_reports = args[2]
        self.df_list = args[3]
        self.inact_days = args[4]
        print(" "); print("Params:")
        print("  completo = ", self.completo)
        print("  envia_sftp = ", self.envia_sftp)
        print("  d_reports = ", self.d_reports)
        print("  df_list = ", self.df_list)
        print("  inact_days = ", self.inact_days)
        print("- MiHilo")

        # El progress bar se considera un 30% para la preparación de los df y el 70% restante es para el procesamiento de los reportes.
        self.prog = f.winbar()
        self.prog.setTitle("Ejecutando reportes")
        self.k_max = len(self.df_list) + sum(x[0] for x in list(self.d_reports.values())) + 1  # el + 1 es por la subida al SFTP.
        print("k_max = ", self.k_max)
        self.prog.max(self.k_max)
        self.prog.show()

    def run(self):
        logging.info("======= INICIO PROCESO REPORTES =======")
        start = datetime.now()

        # -----------------------------------------------------------
        # Genero los dfs comunes a los reportes seleccionados.
        # -----------------------------------------------------------
        print("+ Genera df comunes a los reportes seleccionados")
        l_adv = 0

        for item in self.df_list:
            if item == "df_ora":
                ora = c.getOracle()
                df_ora = ora.get_actives()
                l_adv += 1
                self.prog.adv(l_adv)
            if item == "df_opq_actives":
                opq = c.getOpecus()
                df_opq_actives = opq.get_actives()
                l_adv += 1
                self.prog.adv(l_adv)
            if item == "df_opq_inactives":
                opq = c.getOpecus()
                df_opq_inactives = opq.get_inactives()
                l_adv += 1
                self.prog.adv(l_adv)
            if item == "df_rrhh":
                print("+ Genera df_rrhh")
                df_rrhh = c.getCSV()
                print("- Genera df_rrhh")
                l_adv += 1
                self.prog.adv(l_adv)
            if item == "df_ad_actives":
                ad = c.getAD()
                df_ad_actives = ad.get_actives()
                l_adv += 1
                self.prog.adv(l_adv)
            if item == "df_ad_inactives":
                ad = c.getAD()
                df_ad_inactives = ad.get_inactives()
                l_adv += 1
                self.prog.adv(l_adv)
            if item == "df_vpn":
                df_vpn = c.getVPN()
                l_adv += 1
                self.prog.adv(l_adv)
            if item == "df_gsuite":
                df_gsuite = c.getGSuite()
                l_adv += 1
                self.prog.adv(l_adv)
            if item == "df_surge":
                df_surge = c.getSurgeMail()
                l_adv += 1
                self.prog.adv(l_adv)
            if item == "df_nomina":
                df_nomina = c.nomina()
                l_adv += 1
                self.prog.adv(l_adv)
        print("- Genera df comunes a los reportes seleccionados")

        # -----------------------------------------------------------
        # Genero los reportes seleccionados.
        # -----------------------------------------------------------
        print("+ EJECUCIÓN REPORTES")

        # self.prog = f.winbar()
        # self.prog.setTitle("Ejecutando los reportes")
        # self.prog.show()

        # Obtengo la cantidad de reportes que fueron selecionados
        # k_reps = 0
        # for x in self.d_reports.values():
        #     if x[0]:
        #         k_reps += 1
        # k_reps = sum(x for x, _ in list(self.d_reports.values())) # esto funciona ok!

        # self.prog.setTitle("Ejecutando Reportes")
        # self.prog.show()
        # self.prog.max(k_reps)
        # self.prog.show()
        # l_adv = 0

        # Llamo y ejecuto cada reporte
        for key, val in self.d_reports.items():
            if key == 'chkRepNomvsOracle' and val[0]:
                print(val[1])
                logging.info("+ Nómina vs Oracle")
                re.reportEndUsers(self, completo=self.completo, envia_sftp=self.envia_sftp, args=(df_ora, df_nomina,))
                logging.info("- Nómina vs Oracle")
                l_adv += 1
                self.prog.adv(l_adv)
            if key == 'chkRepOraDBUsers' and val[0]:
                logging.info("+ Oracle DB Users")
                db.reportDbUsers(self, completo=self.completo, envia_sftp=self.envia_sftp, args=(df_rrhh,))
                logging.info("- Oracle DB Users")
                l_adv += 1
                self.prog.adv(l_adv)
            if key == "chkRepOraApplUsers" and val[0]:
                logging.info("+ Oracle Appl Users")
                # rau.reportApplUsers(self, envia_sftp=self.envia_sftp)
                rov.reportApplUsers(self, envia_sftp=self.envia_sftp)
                logging.info("- Oracle Appl Users")
                l_adv += 1
                self.prog.adv(l_adv)
            if key == 'chkRepOraInactivity' and val[0]:
                logging.info("+ Oracle Inactivity Report")
                ri.reportInactivity(self, inactive_days=self.inact_days, completo=self.completo, envia_sftp=self.envia_sftp, args=(df_ora, df_nomina, df_vpn))
                logging.info("- Oracle Inactivity Report")
                l_adv += 1
                self.prog.adv(l_adv)
            if key == 'chkRepOraSuperUser' and val[0]:
                logging.info("+ Oracle Super Users Report")
                rov.reportSuperUsers(self, envia_sftp=self.envia_sftp)
                logging.info("- Oracle Super Users Report")
                l_adv += 1
                self.prog.adv(l_adv)
            if key == 'chkRepOraConfigurador' and val[0]:
                logging.info("+ Oracle Configurador Report")
                rov.reportConfigurador(self, envia_sftp=self.envia_sftp)
                logging.info("- Oracle Configurador Report")
                l_adv += 1
                self.prog.adv(l_adv)
            if key == 'chkRepOraUsersCreation' and val[0]:
                logging.info("+ Oracle Users Creation Report")
                rov.reportUsersCreation(self, envia_sftp=self.envia_sftp)
                logging.info("- Oracle Users Creation Report")
                l_adv += 1
                self.prog.adv(l_adv)
            if key == 'chkRepOraRespFST' and val[0]:
                logging.info("+ Oracle FST Responsibilities Report")
                rov.reportRespEquipoFST(self, envia_sftp=self.envia_sftp)
                logging.info("- Oracle FST Responsibilities Report")
                l_adv += 1
                self.prog.adv(l_adv)
            if key == 'chkRepNominavsAD' and val[0]:
                logging.info("+ Nómina vs AD")
                nad.rep_nomina_vs_ad(self, completo=self.completo, envia_sftp=self.envia_sftp, args=(df_ad_actives, df_nomina,))
                logging.info("- Nómina vs AD")
                l_adv += 1
                self.prog.adv(l_adv)
            if key == 'chkRepNominavsVPN' and val[0]:
                logging.info("+ Nómina vs VPN")
                nvpn.rep_nomina_vs_vpn(self, completo=self.completo, envia_sftp=self.envia_sftp, args=(df_vpn, df_nomina,))
                logging.info("- Nómina vs VPN")
                l_adv += 1
                self.prog.adv(l_adv)
            if key == 'chkRepNominavsGSuite' and val[0]:
                logging.info("+ Nómina vs GSuite")
                ngs.rep_nomina_vs_gsuite(self, completo=self.completo, envia_sftp=self.envia_sftp, args=(df_gsuite, df_nomina,))
                logging.info("- Nómina vs GSuite")
                l_adv += 1
                self.prog.adv(l_adv)
            if key == 'chkRepNominavsSurgeMail' and val[0]:
                logging.info("+ Nómina vs SurgeMail")
                nsm.rep_nomina_vs_surgemail(self, completo=self.completo, envia_sftp=self.envia_sftp, args=(df_surge, df_nomina,))
                logging.info("- Nómina vs SurgeMail")
                l_adv += 1
                self.prog.adv(l_adv)
            if key == 'chkRepNominavsATP1' and val[0]:
                logging.info("+ Nómina vs ATP1")
                natp1.rep_nomina_vs_atp1(self, completo=self.completo, envia_sftp=self.envia_sftp, args=(df_nomina,))
                logging.info("- Nómina vs ATP1")
                l_adv += 1
                self.prog.adv(l_adv)
            if key == 'chkRepNominavsATP1PCI' and val[0]:
                logging.info("+ Nómina vs ATP1-PCI")
                natp1pci.rep_nomina_vs_atp1pci(self, completo=self.completo, envia_sftp=self.envia_sftp, args=(df_nomina,))
                logging.info("- Nómina vs ATP1-PCI")
                l_adv += 1
                self.prog.adv(l_adv)
            if key == 'chkRepNominavsATP3' and val[0]:
                logging.info("+ Nómina vs ATP3")
                natp3.rep_nomina_vs_atp3(self, completo=self.completo, envia_sftp=self.envia_sftp, args=(df_nomina,))
                logging.info("- Nómina vs ATP3")
                l_adv += 1
                self.prog.adv(l_adv)
            if key == 'chkRepNominavsATP3PCI' and val[0]:
                logging.info("+ Nómina vs ATP3-PCI")
                natp3pci.rep_nomina_vs_atp3pci(self, completo=self.completo, envia_sftp=self.envia_sftp, args=(df_nomina,))
                logging.info("- Nómina vs ATP3-PCI")
                l_adv += 1
                self.prog.adv(l_adv)
            if key == 'chkRepNominavsSOX' and val[0]:
                logging.info("+ Nómina vs SOX Applications")
                nsox.rep_nomina_vs_sox(self, completo=self.completo, envia_sftp=self.envia_sftp, args=(df_nomina,))
                logging.info("- Nómina vs SOX Applications")
                l_adv += 1
                self.prog.adv(l_adv)
            if key == 'chkRepNominavsPS' and val[0]:
                logging.info("+ Nómina vs PeopleSoft")
                nps.rep_nomina_vs_ps(self, completo=self.completo, envia_sftp=self.envia_sftp, args=(df_nomina,))
                logging.info("- Nómina vs PeopleSoft")
                l_adv += 1
                self.prog.adv(l_adv)
            if key == 'chkRepADCompleto' and val[0]:
                logging.info("+ AD Inactivity Report (Actives + Inactives)")
                rad.reportADInactivity(self, completo=True, envia_sftp=self.envia_sftp, args=(df_vpn, df_gsuite, df_surge))
                logging.info("- AD Inactivity Report (Actives + Inactives)")
                l_adv += 1
                self.prog.adv(l_adv)
            if key == 'chkRepNomina' and val[0]:
                logging.info("+ Nómina Report")
                nom.rep_nomina(self, envia_sftp=self.envia_sftp, args=(df_nomina,))
                logging.info("- Nómina Report")
                l_adv += 1
                self.prog.adv(l_adv)
            if key == 'chkRepADInactivity' and val[0]:
                logging.info("+ AD Inactivity Report (Actives)")
                rad.reportADInactivity(self, completo=False, envia_sftp=self.envia_sftp, args=(df_vpn, df_gsuite, df_surge))
                logging.info("- AD Inactivity Report (Actives)")
                l_adv += 1
                self.prog.adv(l_adv)
        print("- EJECUCIÓN REPORTES")

        # ----------------------------------------------
        # Sube los reportes al SFTP.
        # ----------------------------------------------
        l_adv += 1
        # print("último l_adv = ", l_adv)
        self.prog.adv(l_adv)
        if self.envia_sftp:
            logging.info("+ Sube archivos al SFTP")
            # print("+ Sube archivos al SFTP")
            f.popQueue()
            logging.info("- Sube archivos al SFTP")
            # print("- Sube archivos al SFTP")
        # self.prog.adv(self.k_max)  # parece que esto es lo que pincha al finalizar el proceso
        self.prog.cl()
        print("cierra el progress_bar")

        print("Processing elapsed time (report_main): {}".format(datetime.now() - start))
        logging.info("======== FIN PROCESO REPORTES =========")
