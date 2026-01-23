import sys
import os
import json
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import URL
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
                             QLabel, QLineEdit, QPushButton, QTextEdit, QFileDialog, 
                             QStackedWidget, QMessageBox, QComboBox, QProgressBar, 
                             QTableWidget, QTableWidgetItem, QHeaderView, QCheckBox, QFrame)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QFont, QColor, QIcon
import logging
import traceback

import phoenix_importer

# [2026-01-19] Anya-Corena: Phoenix SQL Importer GUI (English + Logging Edition)

# Configure Logging for GUI
log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "phoenix_debug.log")
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("PhoenixGUI")

def exception_hook(exctype, value, tb):
    """Global hook to catch unhandled exceptions and log them."""
    error_msg = "".join(traceback.format_exception(exctype, value, tb))
    logger.critical(f"Unhandled Exception:\n{error_msg}")
    print(error_msg)
    sys.__excepthook__(exctype, value, tb)

sys.excepthook = exception_hook

def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

class WorkerThread(QThread):
    progress_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(bool, str)

    def __init__(self, params):
        super().__init__()
        self.params = params

    def run(self):
        try:
            logger.info("Starting Worker Thread Import Process")
            try:
                p = int(self.params['port'])
            except:
                p = 5432

            engine = phoenix_importer.get_engine(
                self.params['user'], self.params['pass'], 
                self.params['host'], p, self.params['db']
            )
            
            with open(self.params['json'], 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            df = pd.DataFrame(data)
            df = df[self.params['selected_columns']] # Filter columns
            
            self.progress_signal.emit(f"[*] Starting Mode: {self.params['mode'].upper()}")
            
            table = self.params['table']
            dtype_map = self.params['dtype_map']

            if self.params['mode'] == 'nuke':
                self.progress_signal.emit("[!] Dropping old table...")
                with engine.connect() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {table}"))
                    conn.commit()
                self.progress_signal.emit("[*] Creating new table...")
                df.to_sql(table, engine, if_exists='replace', index=False, dtype=dtype_map)
                
                # PK Logic
                with engine.connect() as conn:
                    pk_field = self.params['pk']
                    if pk_field and pk_field in df.columns:
                        try:
                            conn.execute(text(f"ALTER TABLE {table} ADD PRIMARY KEY ({pk_field});"))
                        except Exception as e:
                            logger.warning(f"Failed to set PK: {e}")
                            self.progress_signal.emit(f"[WARN] Could not set PK (Duplicates?): {e}")
                    else:
                        conn.execute(text(f"ALTER TABLE {table} ADD COLUMN id SERIAL PRIMARY KEY;"))
                    conn.commit()
            
            elif self.params['mode'] == 'upsert':
                self.progress_signal.emit("[*] Processing UPSERT strategy...")
                phoenix_importer.process_data(
                    self.params['json'], table, engine, 'upsert', 
                    self.params['pk'], gui_callback=self.progress_signal.emit
                )
            
            elif self.params['mode'] == 'append':
                self.progress_signal.emit("[*] Appending data...")
                df.to_sql(table, engine, if_exists='append', index=False, dtype=dtype_map)

            logger.info("Import Process Finished Successfully")
            self.finished_signal.emit(True, "Process finished successfully!")
        except Exception as e:
            logger.exception("Worker Thread Crash")
            self.finished_signal.emit(False, str(e))

class PhoenixApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Phoenix SQL Importer (English Edition)")
        self.setMinimumSize(1000, 750)
        
        icon_path = resource_path("icon.png")
        if os.path.exists(icon_path):
            self.setWindowIcon(QIcon(icon_path))
        
        self.central = QWidget()
        self.setCentralWidget(self.central)
        self.layout = QVBoxLayout(self.central)
        
        # 1. STEP BAR
        self.step_layout = QHBoxLayout()
        self.step_labels = []
        steps = ["1. JSON File", "2. Connection", "3. Schema", "4. Config", "5. Execute"]
        for s in steps:
            lbl = QLabel(s)
            lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
            lbl.setStyleSheet("background-color: #DDD; color: #555; padding: 8px; border-radius: 4px; font-weight: bold;")
            self.step_layout.addWidget(lbl)
            self.step_labels.append(lbl)
        self.layout.addLayout(self.step_layout)
        
        # 2. STACK
        self.stack = QStackedWidget()
        self.layout.addWidget(self.stack)
        
        self.apply_styles()
        self.setup_ui()
        self.current_step = 0
        self.update_step_visuals()
        self.df = None
        logger.info("GUI Initialized")

    def apply_styles(self):
        self.setStyleSheet("""
            QMainWindow { background-color: #FDFBF7; }
            QLabel { font-family: 'Segoe UI'; font-size: 14px; color: #333; }
            QLabel#Header { font-size: 22px; font-weight: bold; color: #B22222; margin-bottom: 10px; }
            QLineEdit, QComboBox, QTableWidget {
                background-color: #FFF; color: #000; border: 1px solid #CCC;
                border-radius: 4px; padding: 6px; font-size: 13px;
            }
            QPushButton {
                background-color: #E0E0E0; border: none; border-radius: 4px;
                padding: 8px 16px; font-weight: bold; color: #333;
            }
            QPushButton:hover { background-color: #D0D0D0; }
            QPushButton#ActionBtn { background-color: #FF8C00; color: white; }
            QPushButton#ActionBtn:hover { background-color: #FF4500; }
            QPushButton#NextBtn { background-color: #2F4F4F; color: white; min-width: 100px; }
            
            QTableWidget::item { padding: 5px; }
            QComboBox::drop-down { border: none; width: 20px; }
            
            QMessageBox { background-color: #FFF; }
            QMessageBox QLabel { color: #000; }
        """)

    def update_step_visuals(self):
        for i, lbl in enumerate(self.step_labels):
            if i == self.current_step:
                lbl.setStyleSheet("background-color: #FF8C00; color: white; padding: 8px; border-radius: 4px; font-weight: bold;")
            else:
                lbl.setStyleSheet("background-color: #EEE; color: #AAA; padding: 8px; border-radius: 4px;")
        
        self.btn_back.setVisible(self.current_step > 0)
        if self.current_step == 4:
            self.btn_next.setVisible(False)
        else:
            self.btn_next.setVisible(True)
            self.btn_next.setText("Next")

    def setup_ui(self):
        # --- PAGE 1: JSON ---
        p1 = QWidget(); l1 = QVBoxLayout(p1)
        l1.addWidget(QLabel("Load Data File", objectName="Header"))
        
        btn_json = QPushButton("Select JSON File", objectName="ActionBtn")
        btn_json.clicked.connect(self.load_json)
        l1.addWidget(btn_json)
        
        self.preview = QTextEdit(); self.preview.setReadOnly(True)
        self.preview.setStyleSheet("color: #000; background-color: #FFF8DC; border: 1px solid #FFD700;")
        l1.addWidget(self.preview)
        self.stack.addWidget(p1)

        # --- PAGE 2: CONN ---
        p2 = QWidget(); l2 = QVBoxLayout(p2)
        l2.addWidget(QLabel("Database Connection", objectName="Header"))
        
        form = QVBoxLayout()
        self.host = QLineEdit("localhost")); form.addWidget(self.host)
        self.port = QLineEdit("[REDACTED_VALUE]")); form.addWidget(self.port)
        self.db = QLineEdit("")); form.addWidget(self.db)
        self.user = QLineEdit("[REDACTED_VALUE]")); form.addWidget(self.user)
        self.pw = QLineEdit("")); form.addWidget(self.pw)
        l2.addLayout(form)
        
        btn_test = QPushButton("Test Connection", objectName="ActionBtn")
        btn_test.clicked.connect(self.test_conn)
        l2.addWidget(btn_test)
        self.stack.addWidget(p2)

        # --- PAGE 3: SCHEMA ---
        p3 = QWidget(); l3 = QVBoxLayout(p3)
        l3.addWidget(QLabel("Field Mapping", objectName="Header"))
        
        self.table_schema = QTableWidget(0, 5)
        self.table_schema.setHorizontalHeaderLabels(["Include", "Field", "JSON Type", "SQL Type", "PK"])
        self.table_schema.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.table_schema.verticalHeader().setDefaultSectionSize(40)
        self.table_schema.setStyleSheet("QHeaderView::section { background-color: #FF8C00; color: white; border: none; }")
        l3.addWidget(self.table_schema)
        self.stack.addWidget(p3)

        # --- PAGE 4: CONFIG ---
        p4 = QWidget(); l4 = QVBoxLayout(p4)
        l4.addWidget(QLabel("Import Configuration", objectName="Header"))
        
        l4.addWidget(QLabel("Target Table Name:"))
        self.table_name = QLineEdit("[REDACTED_VALUE]")
        l4.addWidget(self.table_name)
        
        l4.addWidget(QLabel("Operation Mode:"))
        self.mode_combo = QComboBox()
        self.mode_combo.addItems(["upsert (update + insert)", "nuke (drop table)", "append (add)"])
        self.mode_combo.currentTextChanged.connect(self.update_mode_desc)
        l4.addWidget(self.mode_combo)
        
        self.mode_desc = QLabel("Updates existing records (by SKU) and inserts new ones.")
        self.mode_desc.setStyleSheet("color: #666; font-style: italic; margin-left: 10px;")
        l4.addWidget(self.mode_desc)
        
        self.stack.addWidget(p4)

        # --- PAGE 5: RUN ---
        p5 = QWidget(); l5 = QVBoxLayout(p5)
        l5.addWidget(QLabel("Execution Console", objectName="Header"))
        
        self.progress = QProgressBar()
        l5.addWidget(self.progress)
        
        self.log = QTextEdit(); self.log.setReadOnly(True)
        self.log.setStyleSheet("background-color: #222; color: #0F0; font-family: Consolas;")
        l5.addWidget(self.log)
        
        btn_run = QPushButton("START IMPORT", objectName="ActionBtn")
        btn_run.setMinimumHeight(50)
        btn_run.clicked.connect(self.run_import)
        l5.addWidget(btn_run)
        self.stack.addWidget(p5)

        # NAV BUTTONS
        nav = QHBoxLayout()
        self.btn_back = QPushButton("Back")
        self.btn_back.clicked.connect(self.go_back)
        nav.addWidget(self.btn_back)
        
        self.btn_next = QPushButton("Next", objectName="NextBtn")
        self.btn_next.clicked.connect(self.go_next)
        nav.addWidget(self.btn_next)
        self.layout.addLayout(nav)

    def update_mode_desc(self, text):
        if "upsert" in text:
            self.mode_desc.setText("ℹ️ UPSERT: Updates if ID/SKU exists. Otherwise, creates it (Safe & Recommended).")
        elif "nuke" in text:
            self.mode_desc.setText("⚠️ NUKE: DROPS the entire table and recreates it (Destructive).")
        elif "append" in text:
            self.mode_desc.setText("➕ APPEND: Only adds to the end. Might fail if duplicates exist.")

    def load_json(self):
        path, _ = QFileDialog.getOpenFileName(self, "Open JSON", "", "JSON (*.json)")
        if path:
            logger.info(f"User selected file: {path}")
            self.json_path = path
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                self.df = pd.DataFrame(data)
                self.preview.setText(json.dumps(data[:2], indent=2))
                self.populate_schema()
            except Exception as e:
                logger.error(f"Error loading JSON: {e}")
                self.preview.setText(f"Error: {e}")

    def populate_schema(self):
        if self.df is None: return
        self.table_schema.setRowCount(len(self.df.columns))
        for i, col in enumerate(self.df.columns):
            # Include Checkbox
            chk = QCheckBox(); chk.setChecked(True); 
            w = QWidget(); l = QHBoxLayout(w); l.addWidget(chk); l.setAlignment(Qt.AlignmentFlag.AlignCenter); l.setContentsMargins(0,0,0,0)
            self.table_schema.setCellWidget(i, 0, w)
            
            # Field Name
            self.table_schema.setItem(i, 1, QTableWidgetItem(col))
            
            # JSON Type
            stype = str(self.df[col].dtype)
            self.table_schema.setItem(i, 2, QTableWidgetItem(stype))
            
            # SQL Combo
            combo = QComboBox()
            combo.addItems(["VARCHAR(255)", "TEXT", "JSONB", "INTEGER", "NUMERIC", "BOOLEAN"])
            
            series = self.df[col].dropna()
            sample = series.iloc[0] if not series.empty else None
            
            if isinstance(sample, (dict, list)): combo.setCurrentText("JSONB")
            elif "int" in stype: combo.setCurrentText("INTEGER")
            elif "float" in stype: combo.setCurrentText("NUMERIC")
            elif "bool" in stype: combo.setCurrentText("BOOLEAN")
            else:
                if not series.empty and series.dtype == 'object':
                    max_len = series.astype(str).map(len).max()
                    combo.setCurrentText("TEXT" if max_len > 255 else "VARCHAR(255)")
                else:
                    combo.setCurrentText("TEXT")
            self.table_schema.setCellWidget(i, 3, combo)

            # PK Checkbox
            pk_chk = QCheckBox(); pk_chk.setChecked(col.lower() in ['sku', 'id'])
            w_pk = QWidget(); l_pk = QHBoxLayout(w_pk); l_pk.addWidget(pk_chk); l_pk.setAlignment(Qt.AlignmentFlag.AlignCenter); l_pk.setContentsMargins(0,0,0,0)
            self.table_schema.setCellWidget(i, 4, w_pk)

    def test_conn(self):
        try:
            p = int(self.port.text())
            url = URL.create("postgresql+psycopg2", username=self.user.text(), password=self.pw.text(), host=self.host.text(), port=p, database=self.db.text())
            engine = create_engine(url)
            with engine.connect() as conn: QMessageBox.information(self, "Success", "Connection established successfully!")
        except Exception as e: 
            logger.error(f"Connection test failed: {e}")
            QMessageBox.critical(self, "Connection Error", str(e))

    def go_next(self):
        if self.current_step == 0:
            if not hasattr(self, 'json_path') or not self.json_path:
                QMessageBox.warning(self, "Missing File", "Please select a JSON file to continue.")
                return
        if self.current_step < 4:
            self.current_step += 1
            self.stack.setCurrentIndex(self.current_step)
            self.update_step_visuals()

    def go_back(self):
        if self.current_step > 0:
            self.current_step -= 1
            self.stack.setCurrentIndex(self.current_step)
            self.update_step_visuals()

    def run_import(self):
        selected_cols = []
        dtype_map = {}
        pk = ""
        for i in range(self.table_schema.rowCount()):
            chk_widget = self.table_schema.cellWidget(i, 0)
            chk = chk_widget.findChild(QCheckBox)
            if chk.isChecked():
                col_name = self.table_schema.item(i, 1).text()
                selected_cols.append(col_name)
                sql_type_str = self.table_schema.cellWidget(i, 3).currentText()
                if sql_type_str == "JSONB": dtype_map[col_name] = sqlalchemy.dialects.postgresql.JSONB
                elif sql_type_str == "INTEGER": dtype_map[col_name] = sqlalchemy.Integer
                elif sql_type_str == "NUMERIC": dtype_map[col_name] = sqlalchemy.Float
                elif sql_type_str == "BOOLEAN": dtype_map[col_name] = sqlalchemy.Boolean
                else: dtype_map[col_name] = sqlalchemy.Text()
                
                pk_widget = self.table_schema.cellWidget(i, 4)
                pk_chk = pk_widget.findChild(QCheckBox)
                if pk_chk.isChecked(): pk = col_name

        params = {
            'json': self.json_path, 'host': self.host.text(), 'port': self.port.text(), 'db': self.db.text(),
            'user': self.user.text(), 'pass': self.pw.text(), 'table': self.table_name.text(),
            'mode': self.mode_combo.currentText().split(" ")[0],
            'selected_columns': selected_cols,
            'dtype_map': dtype_map, 'pk': pk
        }
        logger.info(f"Starting import: {params['mode']} on {params['table']}")
        self.worker = WorkerThread(params)
        self.worker.progress_signal.connect(self.update_log)
        self.worker.finished_signal.connect(lambda s, m: QMessageBox.information(self, "Status", m))
        self.worker.start()

    def update_log(self, msg):
        self.log.append(msg)
        self.log.verticalScrollBar().setValue(self.log.verticalScrollBar().maximum())

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = PhoenixApp()
    window.show()
    sys.exit(app.exec())
