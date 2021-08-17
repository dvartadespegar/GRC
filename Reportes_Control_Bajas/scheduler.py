import pandas as pd
import threading
from PyQt5 import uic
from PyQt5.QtWidgets import QDialog
import datetime as dt
import time as tm
import funcs as f
import connections as c
import report_db as db


#pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

# logging.basicConfig(level=logging.DEBUG, format='(%(threadName)-9s) %(message)s',)

class Scheduler(QDialog):
    def __init__(self):
        print("+ Scheduler - Init")
        QDialog.__init__(self)
        uic.loadUi("screens/scr_scheduler.ui", self)
        # Botones del form
        self.tblScheduler.setFocus()
        self.btnSave.clicked.connect(self.saveSched)
        self.btnCancel.clicked.connect(self.cancelSched)
        self.btnExit.clicked.connect(self.close_ui)
        self.btnNew.clicked.connect(self.newSched)
        # Procedimientos
        print("  + getUserDB (abre la BD)")
        self.db = c.getUsersDB()    # Abre la base de datos
        print("  - getUserDB (abre la BD)")
        print("  + loadReports()")
        self.loadReports()          # Carga el contenido del ComboBox de los reportes a schedular.
        print("  - loadReports()")
        print("  + loadSched")
        self.loadSched()            # Carga la pantalla con los datos del schedule que ya tenía en la BD.
        print("  - loadSched")
        print("- Scheduler - Init")


    def newSched(self):
        self.cboReport.enabled(True)
        self.chkCompleteFlag.enabled(True)
        self.rbDias.enabled(True)
        self.rbPeriodico.enabled(True)
        self.rbPeriodico.isChecked()
        self.txtExecuteEach.enabled(True)
        self.cboTimeUnit.enabled(True)
        self.dtStart.enabled(True)
        self.dtEnd.enabled(True)
        self.btnCancel.enabled(True)
        self.btnSave.enabled(True)
        self.cboReport.setFocus()

    def cancelSched(self):
        self.tblScheduler.setFocus()
        self.cboReport.enabled(False)
        self.chkCompleteFlag.enabled(False)
        self.rbDias.enabled(False)
        self.rbPeriodico.enabled(False)
        self.chkD.enabled(False)
        self.chkL.enabled(False)
        self.chkM.enabled(False)
        self.chkW.enabled(False)
        self.chkJ.enabled(False)
        self.chkV.enabled(False)
        self.chkS.enabled(False)
        self.txtExecuteEach.enabled(False)
        self.cboTimeUnit.enabled(False)
        self.dtStart.enabled(False)
        self.dtEnd.enabled(False)
        self.btnCancel.enabled(False)
        self.btnSave.enabled(False)

    def loadReports(self):
        print("+ loadReports")
        # Carga el ComboBox de los reportes a schedular.
        df = self.db.getData("Select report_name From Reports Where enabled_flag = 'Y';")
        for r in range(0, len(df.index)):
            self.cboReport.addItem(df.iloc[r, 0])
        print("- loadReports")


    def loadSched(self):
        print("+ loadSched")
        """ esto funciona: era para cargar con datos de base todos los campos de lo que ahora es el new (para cargar un nuevo schedule).
            toma el reporte que está informado en cboReport y trae de la tabla el 1er registro del schedule, y llena los campos.
        content = self.cboReport.currentText()
        print("content =", content)
        df_report = self.db.getData("Select report_id From Reports Where report_name = '"+content+"';")
        print("df_report = "); f.pprint(df_report)
        print("df_report.iloc[0,0] =", df_report.iloc[0,0])
        df = self.db.getData("Select * From Scheduler Where enabled_flag = 'Y' and report_id = "+str(df_report.iloc[0, 0])+";")
        print("df = "); f.pprint(df)
        mapeo = {"D": "Día/s", "M": "Mes/es", "S": "Semana/s", "H": "Hora/s", "M": "Minuto/s"}

        if len(df.index) > 0:
            self.chkCompleteFlag.setChecked(True if df.loc[0, "Complete_Report_Flag"] == "Y" else False)
            self.txtExecuteEach.setText(str(df.loc[0, "Execute_Each"]))
            index = self.cboTimeUnit.findText(mapeo[df.loc[0, "Time_Unit"]], QtCore.Qt.MatchFixedString)
            if index >= 0:
                self.cboTimeUnit.setCurrentIndex(index)
            self.dtBegin = df.loc[0, "Start_Date"]
            self.dtEnd = df.loc[0, "End_Date"]
        else:
            self.chkCompleteFlag.setChecked(False)
            self.txtExecuteEach.setText("0")
            index = self.cboTimeUnit.findText(mapeo["S"], QtCore.Qt.MatchFixedString)
            if index >= 0:
                self.cboTimeUnit.setCurrentIndex(index)
            self.dtStart.setDateTime(datetime.now())
            self.dtEnd.setDateTime(datetime.now())
        """
        df = self.db.getData("Select * From Scheduler Where enabled_flag = 'Y';")
        print("df = "); print(df)
        mapeo = {"D": "Día/s", "M": "Mes/es", "S": "Semana/s", "H": "Hora/s", "N": "Minuto/s"}
        df["Time_Unit"] = df["Time_Unit"].apply(lambda x: mapeo[x])

        # ===============================
        # CARGO REGISTROS EN PANTALLA.
        # ===============================
        borg = f.Borg()
        borg.df = df
        borg.df2 = df
        f.showTable(self=self, isChecked=False, qtable=self.tblScheduler)     # OJO! que tblOra es el parámetro del Hilo, no es el nombre del QtTableWidget.
        print("- loadSched")


    def saveSched(self):
        print("+ saveSched")
        df = self.db.getData("Select report_id From Reports Where report_name = '"+self.cboReport.currentText()+"';")
        mapeo = {"Día/s": "D", "Mes/es": "M", "Semana/s": "S", "Hora/s": "H", "Minuto/s": "M"}

        print("report_id = ", df.iloc[0, 0])
        print("complete_report_flag = ", self.chkCompleteFlag.isChecked())
        print("execute_each = ", self.txtExecuteEach.text())
        print("time_unit = ", self.cboTimeUnit.currentText())
        print("time_unit (con mapeo) = ", mapeo[self.cboTimeUnit.currentText()])

        try:
            l_start = self.dtStart.dateTime().toString(self.dtStart.displayFormat())
            l_end = self.dtEnd.dateTime().toString(self.dtEnd.displayFormat())
            print("start_date = ", l_start)
            print("end_date = ", l_end)
        except Exception as err:
            print("ERROR al convertir las fechas; ", err)

        self.db.insert(report_id=df.iloc[0, 0], complete_report_flag=self.chkCompleteFlag.isChecked() , execute_each=self.txtExecuteEach.text(),
                       time_unit=mapeo[self.cboTimeUnit.currentText()], execute_days=None , start_date=l_start, end_date=l_end)
        print("- saveSched")

        # Cierro la ventana y finalizo.
        self.db.close()
        self.close()


    def close_ui(self):
        print("+ Cierra Scheduler")
        self.db.close()
        self.close()
        print("- Cierra Scheduler")


    def activaSch(self):
        print("/======= INICIO ACTIVA SCHEDULES =======/")

        print("+ HiloInact")
        hilo = HiloSched(args=(), daemon=False)
        print("- HiloInact")
        hilo.start()

        print("isAlive? (después del while) = ", hilo.is_alive())
        print("/======= FIN ACTIVA SCHEDULES =======/")


    def desactivaSch(self):
        print("/======= INICIO DESACTIVA SCHEDULES =======/")
        self.rt.stop()


class HiloSched(threading.Thread):
    def __init__(self, group=None, target=None, name=None, args=(), kwargs=None, *, daemon=True):
        super().__init__(group=group, target=target, name=name, daemon=daemon)
        self.base = c.getUsersDB()    # Abre la base de datos

    def run(self):
        print("+ Entró a RUN (Threading = ", threading.current_thread().getName())
        self.base = c.getUsersDB()    # Abre la base de datos
        sql = "SELECT s.Schedule_Id, r.Report_Id, r.Report_Name, r.Execution_Program_Name, s.Complete_Report_Flag " \
              "      ,s.Execute_Each, s.Time_Unit, s.Execute_Days, s.Start_Date, s.End_Date, s.Next_Execution, s.Status " \
              "  FROM Reports r, Scheduler s " \
              " WHERE r.Report_Id = s.Report_Id " \
              "   AND s.Enabled_Flag = 'Y';"
        print(sql)
        df = self.base.getData(sql)
        print("df = "); print(df)

        # --------------------------------------------------------
        print(" "); print("+ Recorre todos los schedules programados.")
        # --------------------------------------------------------
        for i in range(0, len(df.index)):
            print("  Schedule_Id = ", df.loc[i, "Schedule_Id"], "; Report_Id = ", df.loc[i, "Report_Id"])
            exec_func = df.loc[i, "Execution_Program_Name"]
            print("  exec_func = ", exec_func)

            # Tiempo del schedule en segundos.
            if df.loc[i, "Time_Unit"] == "S":
                delta = dt.timedelta(weeks=int(df.loc[i, "Execute_Each"]))
            elif df.loc[i, "Time_Unit"] == "D":
                delta = dt.timedelta(days=int(df.loc[i, "Execute_Each"]))
            elif df.loc[i, "Time_Unit"] == "H":
                delta = dt.timedelta(hours=int(df.loc[i, "Execute_Each"]))
            elif df.loc[i, "Time_Unit"] == "M":
                delta = dt.timedelta(minutes=int(df.loc[i, "Execute_Each"]))
            else:
                delta = dt.timedelta(seconds=1)
            print("  delta = ", str(delta))
            delta_secs = delta.total_seconds()
            print("  delta (en segundos) = ", delta_secs)

            # --------------------------------
            # Calcula la próxima ejecución.
            # --------------------------------
            hoy = dt.datetime.now()
            print("  Hoy = ", str(hoy))
            next_base = dt.datetime.strptime(df.loc[i, "Next_Execution"], "%d/%m/%Y %H:%M")
            print("  Next_Exec (en base): ", str(next_base))
            hora = int(dt.datetime.strftime(next_base, "%H"))
            minutos = int(dt.datetime.strftime(next_base, "%M"))
            print("  Hora:Minutos: ", hora, ":", minutos)
            if (hoy + delta) > dt.datetime.now():
                next_exec = hoy.replace(day=hoy.day+1, hour=hora, minute=minutos, second=0, microsecond=0) + delta  #timedelta(days=0)
            else:
                next_exec = hoy.replace(day=hoy.day, hour=hora, minute=minutos, second=0, microsecond=0) + delta  #timedelta(days=0)
            waiting_secs = (next_exec - hoy).total_seconds()
            print("  Next Execution = ", next_exec)
            print("  Segundos de espera hasta la próxima ejecución: ", waiting_secs)
            print("  Periodicidad (en segundos) del schedule = ", delta_secs)

            # -------------------------------------------------------------------------------------------------
            # Si la fecha/hora de ejecución es anterior a la fecha actual, ejecuto la función inemediatamente.
            # -------------------------------------------------------------------------------------------------
            if dt.datetime.strptime(df.loc[i, "Next_Execution"], "%d/%m/%Y %H:%M%S") <= dt.datetime.now():
                print("+ Ejecuta la función inmediatamente")
                first_time_secs = 30
                self.rt = RepeatedTimer(first_time_secs, eval(exec_func), (self, 'N', None, ))
                #
                # Actualiza la próxima ejecución en la tabla Scheduler
                self.base.update_next_exec(schedule_id=str(df.loc[i, "Schedule_Id"]), next_exec_str=dt.datetime.strftime(next_exec, "%d/%m/%Y %H:%M"))
                #
                # Paro el schedule
                tm.sleep(first_time_secs)
                print("  Paro el schedule (Stop)")
                self.rt.stop()
                print("- Ejecuta la función inmediatamente")

            # --------------------------------------------------------------------------------------------
            # Schedulo los programas habilitados
            # --------------------------------------------------------------------------------------------
            # Si seleccionó schedular en cada tantos <unit_time> (por ej.: 1 vez por semana)...
            if pd.isna(df.loc[i, "Execute_Days"]):
                print("  + Seleccionó ejecutar cada ", df.loc[i, "Execute_Each"], df.loc[i, "Time_Unit"])

                # Hay que esperar hasta llegar al horario de Start_Date (de la tabla Scheduler).
                tm.sleep(waiting_secs)

                self.rt = RepeatedTimer(delta_secs, eval(exec_func), args=(self, 'N', None, ), kwargs={})
                print("  Se scheduló el reporte: ", df.loc[i, "Report_Name"], "para el", str(next_exec))

                print("  - Seleccionó ejecutar cada ", df.loc[i, "Execute_Each"], df.loc[i, "Time_Unit"])
            print("- Recorre todos los schedules programados."); print(" ")

        print("- Entró a RUN")



# ---------------------------------------------------------------
# Procedimiento "Activar Schedules" que se activa desde main.
# ---------------------------------------------------------------
# def activarSchedules(self):
#     print("+ activarSchedules")
#
#     base = c.getUsersDB()  # Abre la base de datos
#     sql = "SELECT s.Schedule_Id, r.Report_Id, r.Report_Name, r.Execution_Program_Name, s.Complete_Report_Flag, s.Execute_Each " \
#           "      ,s.Time_Unit, s.Execute_Days, s.Start_Date, s.End_Date, s.Next_Execution, s.Status " \
#           "  FROM Reports r, Scheduler s " \
#           " WHERE r.Report_Id = s.Report_Id " \
#           "   AND s.Enabled_Flag = 'Y';"
#     print(sql)
#     df = base.getData(sql)
#     print("df = "); print(df)
#     #mapeo = {"D": "Día/s", "M": "Mes/es", "S": "Semana/s", "H": "Hora/s", "N": "Minuto/s"}
#     # df["Time_Unit"] = df["Time_Unit"].apply(lambda x: mapeo[x])
#
#     print("+ Recorre todos los schedules programados.")
#     for i in range(0, len(df.index)):
#         print(df.loc[i])
#         active_flag = 'N'
#         # Si seleccionó schedular en cada tantos <unit_time> (por ej.: 1 vez por semana)...
#         if pd.isna(df.loc[i, "Execute_Days"]):
#             # schedule.every(df.loc[0, "Execute_Each"]).hours.do(base.reportDbUsers)
#             exec_func = df.loc[i, "Execution_Program_Name"]
#             print("exec_func(1) = ", exec_func)
#             exec(exec_func)
#             #p1 = Periodic(func=eval(exec_func), period=30, args=(df.loc[i, "Complete_Report_Flag"]), kwargs={})
#             #p1.start()
#         else:  # Si seleccionó schedular en determinados días...
#             exec_func = df.loc[i, "Execution_Program_Name"]
#             print("exec_func(2) = ", exec_func, "; Each = ", df.loc[i, "Execute_Each"])
#
#             # Tiempo del schedule en segundos.
#             if df.loc[i, "Time_Unit"] == "S":
#                 elapsed_time = df.loc[i, "Execute_Each"] * (7 * 24 * 60 * 60)
#             elif df.loc[i, "Time_Unit"] == "D":
#                 elapsed_time = df.loc[i, "Execute_Each"] * (24 * 60 * 60)
#             elif df.loc[i, "Time_Unit"] == "H":
#                 elapsed_time = df.loc[i, "Execute_Each"] * (60 * 60)
#             elif df.loc[i, "Time_Unit"] == "M":
#                 elapsed_time = df.loc[i, "Execute_Each"] * 60
#
#             # Si la fecha/hora de ejecución es anterior a la fecha actual, ejecuto la función inemediatamente.
#             if df.loc[i, "Next_Execution"] <= datetime.now():
#                 exec(exec_func)
#
#
#             try:
#                 # p1 = Periodic(func=eval(exec_func), period=1.0, args=(self, 1,), kwargs={})
#                 #p1 = Periodic(func=repOracleDB, period=11.0, args=(self, 1, ), kwargs={})       # funciona, pero la 1a vez da error, parece que lo lanza sin argumentos.
#                 # p1 = Periodic(func=db.reportDbUsers, period=elapsed_time, args=(self, ), kwargs={})       # funciona igual que el anterior.
#                 next_execution = datetime.now()
#                 p1 = Periodic(func=eval(exec_func), period=elapsed_time, args=(self, 'N', ), kwargs={})       # funciona igual que el anterior.
#                 p1.start()
#                 # schedule.every(3).seconds.do(repOracleDB, self)
#                 #chedule.every(30).seconds.do(db.reportDbUsers, self)   # ESTO FUNCiONA, pero no usa threads!!!
#             except Exception as err:
#                 print("ERROR al ejecutar Periodic; ", err)
#
#     print("- Recorre todos los schedules programados.")
#     print("- activarSchedules")
#
#     while 1:
#         schedule.run_pending()
#         print("  esperando... ", threading.currentThread())
#         tt.sleep(1),



def repOracleDB(self, complete="N"):
    print("datetime = ")#, str(datetime.now()))
    # print("sch_id = ", sch_id)

    try:
        job_func = db.reportDbUsers(self, complete, None)

        job_thread = threading.Thread(target=job_func)
        job_thread.start()

    except Exception as err:
        print("ERROR al ejecutar db.reportdbUsers(); ", err)



# class Periodic(object):
#     def __init__(self, func, period, args=[], kwargs={}):
#         logging.debug("  + entró en el init de Periodic")
#         self.period = period
#         self.func = func
#         # logging.debug("    func = ", str(func))
#         # logging.debug("    period = ", str(period))
#         # logging.debug("    args = ", str(args))
#         # logging.debug("    kwargs = ", str(kwargs))
#         self.args = args
#         self.kwargs = kwargs
#         self.event = Event()
#         # logging.debug("    event = ", str(self.event))
#         logging.debug("  - entró en el init de Periodic")
#
#     def start(self):
#         logging.debug("  + entró en start para el _doit = (no lo pongo porque la 1a vez da error ") #, str(self._doit))
#         self.event.clear()
#         self.proc = Thread(target=self._doit, daemon=True)   # --> aquí se lanza el proc que necesito ejecutar.
#         self.proc.start()
#         logging.debug("  - entró en start")
#
#     def stop(self):
#         logging.debug("  + entró en stop")
#         self.event.set()
#         self.proc.join()
#         logging.debug("  - entró en stop")
#
#     def _doit(self):
#         logging.debug("  + entró en _doit")
#         while True:
#             self.event.wait(self.period)                # --> aquí es donde se queda esperando según lo informado en la llamada a Periodic.
#             if self.event.is_set():                     # devuelve true si y solo si el flag interno es true.
#                 logging.debug("    va a hacer el break")
#                 break
#             logging.debug("    ejecuta self.func (estoy en _doit)")
#             self.func(*self.args, **self.kwargs)        # --> esto ejecuta la función del reporte
#         logging.debug("  - entró en _doit")




class RepeatedTimer(object):
    def __init__(self, interval, function, *args, **kwargs):
        print("+ Entró al __init__ de RepeatedTimer")
        self._timer = None
        self.interval = interval
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.is_running = False
        self.next_call = tm.time()
        self.start()
        print("- Entró al __init__ de RepeatedTimer")

    def _run(self):
        self.is_running = False
        self.start()
        self.function(*self.args, **self.kwargs)

    def start(self):
        print("+ Entró en start; self.is_running = ", self.is_running)
        if not self.is_running:
            self.next_call += self.interval
            # self._timer = threading.Timer(self.next_call - time.time(), self._run)
            self._timer = threading.Timer(self.interval, self._run)
            self._timer.start()
            self.is_running = True
        print("- Entró en start")

    def stop(self):
        self._timer.cancel()
        self.is_running = False
