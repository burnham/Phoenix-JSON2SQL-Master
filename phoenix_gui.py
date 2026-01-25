import sys
import os
import json
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import URL
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
    QLabel, QLineEdit, QPushButton, QTextEdit, QFileDialog, 
    QStackedWidget, QMessageBox, QComboBox, QProgressBar, 
    QTableWidget, QTableWidgetItem, QHeaderView, QCheckBox, QFrame,
    QRadioButton, QScrollArea, QGridLayout, QStackedLayout, QGraphicsOpacityEffect
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QFont, QColor, QIcon, QPixmap
import logger_config
import traceback

import phoenix_importer

import ctypes  # ADDED

# [2026-01-19] Anya-Corena: Phoenix SQL Importer GUI (Hardened Edition)

# Configure Logging using centralized system
logger = logger_config.setup_logger("PhoenixGUI")

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

            engine = None
            if not self.params.get('export_path'):
                engine = phoenix_importer.get_engine(
                    self.params['user'], self.params['pass'], 
                    self.params['host'], p, self.params['db']
                )
            
            # Delegate all complexity to phoenix_importer.process_data
            phoenix_importer.process_data(
                json_path=self.params['json'],
                table_name=self.params['table'],
                engine=engine,
                mode=self.params['mode'],
                pk_field=self.params['pk'],
                gui_callback=self.progress_signal.emit,
                export_path=self.params.get('export_path')
            )

            logger.info("Import Process Finished Successfully")
            self.finished_signal.emit(True, "Process finished successfully!")
        except Exception as e:
            logger.exception("Worker Thread Crash")
            self.finished_signal.emit(False, str(e))

class PhoenixApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Phoenix SQL Importer v5.0 (Gold Master)")
        self.setMinimumSize(1000, 750)
        
        icon_path = resource_path("resources/phoenix_icon.ico")
        if os.path.exists(icon_path):
            self.setWindowIcon(QIcon(icon_path))
        
        self.central = QWidget()
        self.setCentralWidget(self.central)
        self.layout = QVBoxLayout(self.central)
        
        # 0. LOGO HEADER - REMOVED per user request for cleaner look.
        # The watermark is now in the preview area.

        # 1. STEP BAR
        self.step_layout = QHBoxLayout()
        self.step_labels = []
        steps = ["1. JSON", "2. Connection", "3. Schema", "4. Config", "5. Execute"]
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
        
        # 3. FOOTER BRANDING
        self.footer = QLabel()
        self.footer.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.footer.setOpenExternalLinks(True)
        
        ln_path = "file:///" + resource_path("resources/linkedin_icon.png").replace("\\", "/")
        gh_path = "file:///" + resource_path("resources/github_icon.png").replace("\\", "/")
        
        self.footer.setText(f"""
            <div style='text-align: center; margin-top: 30px; border-top: 1px solid #EEE; padding-top: 20px;'>
                <p style='font-size: 11px; color: #888; text-transform: uppercase; letter-spacing: 1.5px; margin-bottom: 8px;'>
                    Secured via Phoenix Engine & PostgreSafe Database
                </p>
                <p style='font-size: 15px; font-weight: bold; color: #222;'>
                    <a href="https://www.linkedin.com/in/gobh/" style="color: #222; text-decoration: none;">
                        <img src="{ln_path}" width="16" height="16" style="vertical-align: middle;">&nbsp;Georgios Burnham H.
                    </a>
                    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span style="color: #DDD;">|</span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
                    <a href="https://github.com/burnham" style="color: #222; text-decoration: none;">
                        <img src="{gh_path}" width="16" height="16" style="vertical-align: middle;">&nbsp;GitHub
                    </a>
                </p>
                <p style='font-size: 13px; color: #555; margin-top: 10px; font-weight: 500;'>
                    IT Support Service | Web Solutions | Immersive 3D Visualization & BIM Expertise | Transforming blueprints<br>
                    into 3D experiences that sell
                </p>
            </div>
        """)
        self.apply_styles()
        self.current_step = 0
        self.df = None
        self.setup_ui()
        self.load_local_secrets()
        
        # Add footer at the very bottom
        self.layout.addWidget(self.footer)
        self.update_step_visuals()
        logger.info("GUI Initialized")

    def load_local_secrets(self):
        """Load credentials from .env if it exists (Local Only)"""
        if getattr(sys, 'frozen', False):
            base_path = os.path.dirname(sys.executable)
        else:
            base_path = os.path.dirname(__file__)
            
        env_path = os.path.join(base_path, ".env")
        if os.path.exists(env_path):
            try:
                with open(env_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if "=" in line and not line.startswith("#"):
                            key, value = line.split("=", 1)
                            if key == "DB_HOST": self.host.setText(value)
                            elif key == "DB_PORT": self.port.setText(value)
                            elif key == "DB_DATABASE": self.db.setText(value)
                            elif key == "DB_USER": self.user.setText(value)
                            elif key == "DB_PASSWORD": self.pw.setText(value)
                logger.info("UI: Credentials loaded from local .env file")
            except Exception as e:
                logger.error(f"UI: Error loading .env: {e}")

    def update_step_visuals(self):
        # 🛡️ Hardened Safety Checks: Prevent initialization crashes
        for attr in ['current_step', 'step_labels', 'btn_back', 'btn_next', 'skip_conn', 'action_target_val']:
            if not hasattr(self, attr):
                return

        # 1. Update Step Bar Colors
        for i, lbl in enumerate(self.step_labels):
            if i == self.current_step:
                lbl.setStyleSheet("background-color: #FF8C00; color: white; padding: 8px; border-radius: 4px; font-weight: bold;")
            else:
                lbl.setStyleSheet("background-color: #DDD; color: #555; padding: 8px; border-radius: 4px; font-weight: bold;")
        
        # 2. Dynamic 'Action Target' Sync (Page 4 Info Label)
        is_skipped = self.skip_conn.isChecked()
        target_text = "EXPORT AS SQL SCRIPT (.sql)" if is_skipped else "DIRECT IMPORT TO DATABASE"
        self.action_target_val.setText(target_text)
        logger.info(f"UI: Action Target updated to: {target_text}")
        
        # 3. Handle Navigation Buttons Visibility
        self.btn_back.setVisible(self.current_step > 0)
        
        # 4. Handle 'Next' vs 'Execute' branding
        if self.current_step == 4:
            self.btn_next.setVisible(False) # Hide next button on the console page
        else:
            self.btn_next.setVisible(True)
            # If on Page 4, the label is 'Execute' equivalent naming
            if self.current_step == 3:
                btn_txt = "Export SQL..." if is_skipped else "Start Import"
                self.btn_next.setText(btn_txt)
            else:
                self.btn_next.setText("Next")

    def resizeEvent(self, event):
        """Dynamic Watermark Scaling: Keeps wings safe from edges."""
        if hasattr(self, 'watermark') and hasattr(self, 'watermark_pixmap') and hasattr(self, 'preview_container'):
            container_size = self.preview_container.size()
            
            # Calculate target size: 92% of container (leaves safe margins)
            target_h = int(container_size.height() * 0.92) 
            target_w = int(container_size.width() * 0.92)

            # Scale original pixmap maintaining aspect ratio
            scaled_pix = self.watermark_pixmap.scaled(
                target_w, target_h, 
                Qt.AspectRatioMode.KeepAspectRatio, 
                Qt.TransformationMode.SmoothTransformation
            )
            
            self.watermark.setPixmap(scaled_pix)
        
        super().resizeEvent(event)

    def apply_styles(self):
        self.setStyleSheet("""
            QMainWindow { background-color: #FDFBF7; }
            QLabel { font-family: 'Segoe UI'; font-size: 15px; color: #333; }
            QLabel#Header { font-size: 24px; font-weight: bold; color: #B22222; margin-bottom: 12px; }
            QLineEdit, QTableWidget {
                background-color: #FFF; color: #000; border: 1px solid #CCC;
                border-radius: 4px; padding: 8px; font-size: 14px;
            }
            QComboBox {
                background-color: #FFF; color: #000; border: 1px solid #CCC;
                border-radius: 4px; padding: 2px 8px; font-size: 11px; min-height: 22px;
            }
            QPushButton {
                background-color: #E0E0E0; border: none; border-radius: 4px;
                padding: 10px 20px; font-weight: bold; color: #333; font-size: 14px;
            }
            QPushButton:hover { background-color: #D0D0D0; }
            QPushButton#ActionBtn { background-color: #FF8C00; color: white; }
            QPushButton#ActionBtn:hover { background-color: #FF4500; }
            QPushButton#NextBtn { background-color: #2F4F4F; color: white; min-width: 120px; font-size: 15px; }
            
            QRadioButton { color: #222; font-size: 15px; spacing: 8px; }
            QRadioButton::indicator { width: 18px; height: 18px; }
            
            QTableWidget::item { padding: 6px; }
            QComboBox::drop-down { border: none; width: 24px; }
            
            QMessageBox { background-color: #FFF; }
            QMessageBox QLabel { color: #000; font-size: 14px; }
        """)


    def setup_ui(self):
        # 0. PRE-INITIALIZE NAV BUTTONS (Avoid initialization race conditions)
        self.btn_back = QPushButton("Back")
        self.btn_back.clicked.connect(self.go_back)
        
        self.btn_next = QPushButton("Next", objectName="NextBtn")
        self.btn_next.clicked.connect(self.go_next)

        # --- PAGE 1: JSON ---
        p1 = QWidget(); l1 = QVBoxLayout(p1)
        l1.addWidget(QLabel("Load JSON File", objectName="Header"))
        
        btn_json = QPushButton("Select JSON", objectName="ActionBtn")
        btn_json.clicked.connect(self.load_json)
        l1.addWidget(btn_json)
        
        # --- Watermark container for preview (StackedLayout for layering) ---
        self.preview_container = QWidget()
        self.preview_container.setMinimumHeight(450) # Increased height for better view
        self.preview_stack = QStackedLayout(self.preview_container)
        self.preview_stack.setStackingMode(QStackedLayout.StackingMode.StackAll)

        # LAYER 1: Watermark label (background)
        self.watermark = QLabel()
        self.watermark.setAlignment(Qt.AlignmentFlag.AlignCenter)
        wm_path = resource_path("resources/phoenix_center.png")
        if os.path.exists(wm_path):
            self.watermark_pixmap = QPixmap(wm_path) # Store ORIGINAL for dynamic scaling
            # Initial placeholder scale - actual scaling happens in resizeEvent
            pix = self.watermark_pixmap.scaled(350, 350, Qt.AspectRatioMode.KeepAspectRatio) 
            self.watermark.setPixmap(pix)
            self.watermark.setScaledContents(False) 
        
        opacity = QGraphicsOpacityEffect(self.watermark)
        opacity.setOpacity(0.16)  # 16% opacity (Doubled from 8%)
        self.watermark.setGraphicsEffect(opacity)
        
        self.preview_stack.addWidget(self.watermark)

        # LAYER 2: Foreground preview (Transparent TextEdit)
        self.preview = QTextEdit()
        self.preview.setReadOnly(True)
        # Transparent background to show watermark
        self.preview.setStyleSheet("color: #000; background-color: transparent; border: 1px solid #FFD700;")
        # Important attribute for transparency
        self.preview.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        
        self.preview_stack.addWidget(self.preview)

        l1.addWidget(self.preview_container)
        self.stack.addWidget(p1)

        # --- PAGE 2: CONN ---
        p2 = QWidget(); l2 = QVBoxLayout(p2)
        l2.addWidget(QLabel("Database Connection", objectName="Header"))
        
        form = QVBoxLayout()
        self.host = QLineEdit("localhost"); form.addWidget(QLabel("Host:")); form.addWidget(self.host)
        self.port = QLineEdit("5432"); form.addWidget(QLabel("Port:")); form.addWidget(self.port)
        self.db = QLineEdit(""); form.addWidget(QLabel("Database:")); form.addWidget(self.db)
        self.user = QLineEdit("postgres"); form.addWidget(QLabel("User:")); form.addWidget(self.user)
        self.pw = QLineEdit(""); self.pw.setEchoMode(QLineEdit.EchoMode.Password); form.addWidget(QLabel("Password:")); form.addWidget(self.pw)
        l2.addLayout(form)
        
        btn_test = QPushButton("Test Connection", objectName="ActionBtn")
        btn_test.clicked.connect(self.test_conn)
        l2.addWidget(btn_test)

        l2.addSpacing(20)
        self.skip_conn = QCheckBox("SKIP CONNECTION (SQL EXPORT MODE ONLY)")
        self.skip_conn.setStyleSheet("font-size: 13px; font-weight: bold; color: #000000;")
        l2.addWidget(self.skip_conn)
        
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
        
        self.mode_desc = QLabel("Updates existing records (via SKU) and inserts new ones.")
        self.mode_desc.setStyleSheet("color: #666; font-style: italic; margin-left: 10px;")
        l4.addWidget(self.mode_desc)

        l4.addSpacing(20)
        l4.addWidget(QLabel("Action Target:", objectName="Header"))
        
        # Information Label (Replaces selectables for better UX)
        self.action_target_val = QLabel("DIRECT IMPORT TO DATABASE")
        self.action_target_val.setStyleSheet("font-size: 13px; font-weight: bold; color: #000000; padding: 5px; background: #FFF9E6; border: 1px dashed #FFD700; border-radius: 4px;")
        l4.addWidget(self.action_target_val)
        
        self.stack.addWidget(p4)

        # --- FINAL SIGNAL CONNECTIONS ---
        self.skip_conn.toggled.connect(self.update_step_visuals)
        # Initial trigger
        self.update_step_visuals()

        # --- PAGE 5: RUN ---
        p5 = QWidget(); l5 = QVBoxLayout(p5)
        l5.addWidget(QLabel("Execution Console", objectName="Header"))
        
        self.progress = QProgressBar()
        l5.addWidget(self.progress)
        
        self.log = QTextEdit(); self.log.setReadOnly(True)
        self.log.setStyleSheet("background-color: #222; color: #0F0; font-family: Consolas;")
        l5.addWidget(self.log)
        
        btn_run = QPushButton("START PROCESS", objectName="ActionBtn")
        btn_run.setMinimumHeight(50)
        btn_run.clicked.connect(self.run_import)
        l5.addWidget(btn_run)
        self.stack.addWidget(p5)

        # NAV BUTTONS LAYOUT
        nav = QHBoxLayout()
        nav.addWidget(self.btn_back)
        nav.addWidget(self.btn_next)
        self.layout.addLayout(nav)

    def update_mode_desc(self, text):
        logger.info(f"UI: User changed mode to: {text}")
        if "upsert" in text:
            self.mode_desc.setText("ℹ️ UPSERT: Updates if ID/SKU exists. Otherwise, creates it (Safe & Recommended).")
        elif "nuke" in text:
            self.mode_desc.setText("⚠️ NUKE: DROPS the entire table and recreates it (Destructive).")
        elif "append" in text:
            self.mode_desc.setText("➕ APPEND: Only adds to the end. Might fail if duplicates exist.")

    def load_json(self):
        path, _ = QFileDialog.getOpenFileName(self, "Open JSON", "", "JSON (*.json)")
        if path:
            logger.info(f"UI: User selected JSON file: {path}")
            self.json_path = path
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                self.df = pd.DataFrame(data)
                self.preview.setText(json.dumps(data[:2], indent=2))
                
                # Auto-fill table name with JSON filename (without extension)
                filename = os.path.basename(path)
                table_name_suggestion = os.path.splitext(filename)[0]
                self.table_name.setText(table_name_suggestion)
                
                self.populate_schema()
            except Exception as e:
                logger.error(f"UI: Error loading JSON: {e}")
                self.preview.setText(f"Error: {e}")

    def populate_schema(self):
        if self.df is None: return
        logger.debug(f"UI: Populating schema for {len(self.df.columns)} columns")
        self.table_schema.setRowCount(len(self.df.columns))
        
        row_count = len(self.df)
        
        for i, col in enumerate(self.df.columns):
            # 1. Include Checkbox
            chk = QCheckBox(); chk.setChecked(True); 
            w = QWidget(); l = QHBoxLayout(w); l.addWidget(chk); l.setAlignment(Qt.AlignmentFlag.AlignCenter); l.setContentsMargins(0,0,0,0)
            self.table_schema.setCellWidget(i, 0, w)
            
            # 2. Field Name
            item_name = QTableWidgetItem(col)
            
            # --- Smart Uniqueness Detection ---
            is_unique = False
            try:
                # nunique() fails on lists/dicts
                unique_count = self.df[col].nunique()
                is_unique = (unique_count == row_count)
                
                if is_unique:
                    item_name.setForeground(QColor("#228B22")) # Green
                    item_name.setToolTip("✅ This field is 100% UNIQUE in this file. (Excellent PK candidate)")
                else:
                    item_name.setToolTip(f"⚠️ Not unique. Contains {row_count - unique_count} duplicates.")
            except Exception:
                # If non-hashable (list/dict), it's definitely not a good PK candidate
                item_name.setToolTip("ℹ️ Contains complex data (lists/dicts). Not a PK candidate.")
            
            self.table_schema.setItem(i, 1, item_name)
            
            # 3. JSON Type
            stype = str(self.df[col].dtype)
            self.table_schema.setItem(i, 2, QTableWidgetItem(stype))
            
            # 4. SQL Type Combo
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

            # 5. PK Checkbox
            pk_chk = QCheckBox()
            # Default suggestion
            is_default_pk = col.lower() in ['sku', 'id', 'url']
            pk_chk.setChecked(is_default_pk)
            
            # If default suggested PK is NOT unique, show warning
            if is_default_pk and not is_unique:
                pk_chk.setStyleSheet("QCheckBox { color: red; }")
                pk_chk.setText(" (Non-unique!)")
            
            pk_chk.stateChanged.connect(lambda state, c=col, u=is_unique: self.on_pk_toggle(state, c, u))
            
            w_pk = QWidget(); l_pk = QHBoxLayout(w_pk); l_pk.addWidget(pk_chk); l_pk.setAlignment(Qt.AlignmentFlag.AlignCenter); l_pk.setContentsMargins(0,0,0,0)
            self.table_schema.setCellWidget(i, 4, w_pk)

    def on_pk_toggle(self, state, col, is_unique):
        if state == 2: # Checked
            logger.info(f"UI: User selected PK: {col}")
            if not is_unique:
                 QMessageBox.warning(self, "Non-Unique Key", 
                                    f"The field '{col}' contains duplicates in this JSON.\n\n"
                                    "Phoenix will automatically deduplicate the data (keeping the last occurrence) "
                                    "before importing, but this might not be what you want.\n\n"
                                    "Consider using a unique field like 'url' if available.")

    def test_conn(self):
        logger.info("UI: User clicked Test Connection")
        
        # Validation: Check for empty fields
        missing = []
        if not self.host.text().strip(): missing.append("Host")
        if not self.port.text().strip(): missing.append("Port")
        if not self.db.text().strip(): missing.append("Database")
        if not self.user.text().strip(): missing.append("User")
        # Password might be empty in some weird configs, but usually required. Let's warn if user is also empty.
        
        if missing:
            QMessageBox.warning(self, "Missing Connection Data", 
                              f"Please fill in the following fields:\n- {', '.join(missing)}\n\n"
                              "A valid database connection is required.")
            return

        try:
            p = int(self.port.text())
            url = URL.create("postgresql+psycopg2", username=self.user.text(), password=self.pw.text(), host=self.host.text(), port=p, database=self.db.text())
            engine = create_engine(url)
            with engine.connect() as conn: 
                logger.info("UI: Connection test: SUCCESS")
                QMessageBox.information(self, "Success", "Connection established successfully!")
        except Exception as e: 
            logger.error(f"UI: Connection test: FAILED - {e}")
            QMessageBox.critical(self, "Connection Error", str(e))

    def go_next(self):
        old_step = self.current_step
        if self.current_step == 0:
            if not hasattr(self, 'json_path') or not self.json_path:
                QMessageBox.warning(self, "Missing File", "Please select a JSON file to continue.")
                return
        if self.current_step == 1:
            # Connection validation before advancing
            if not self.skip_conn.isChecked():
                # Check if required fields are filled
                missing = []
                if not self.host.text().strip(): missing.append("Host")
                if not self.port.text().strip(): missing.append("Port")
                if not self.db.text().strip(): missing.append("Database")
                if not self.user.text().strip(): missing.append("User")
                
                if missing:
                    QMessageBox.warning(
                        self, 
                        "Missing Connection Data",
                        f"Please fill in the following fields:\n- {', '.join(missing)}\n\nA valid database connection is required."
                    )
                    return
            
        if self.current_step < 4:
            self.current_step += 1
            logger.info(f"UI: Navigation NEXT - {old_step} -> {self.current_step}")
            self.stack.setCurrentIndex(self.current_step)
            
            # Smart Logic handled in update_step_visuals
            self.update_step_visuals()
            
            # Update Execute button text
            if self.current_step == 4:
                if self.skip_conn.isChecked():
                    self.btn_next.setText("Export SQL...")
                else:
                    self.btn_next.setText("Start Import")

    def go_back(self):
        old_step = self.current_step
        if self.current_step > 0:
            self.current_step -= 1
            logger.info(f"UI: Navigation BACK - {old_step} -> {self.current_step}")
            self.stack.setCurrentIndex(self.current_step)
            self.update_step_visuals()
            self.btn_next.setText("Next")
            self.btn_next.setVisible(True)

    def run_import(self):
        logger.info("UI: User clicked START IMPORT")
        selected_cols = []
        pk = ""
        for i in range(self.table_schema.rowCount()):
            chk_widget = self.table_schema.cellWidget(i, 0)
            chk = chk_widget.findChild(QCheckBox)
            if chk.isChecked():
                col_name = self.table_schema.item(i, 1).text()
                selected_cols.append(col_name)
                
                pk_widget = self.table_schema.cellWidget(i, 4)
                pk_chk = pk_widget.findChild(QCheckBox)
                if pk_chk.isChecked(): pk = col_name

        mode_text = self.mode_combo.currentText()
        mode = mode_text.split(" ")[0]
        table_name = self.table_name.text()
        
        # --- Pre-run Confirmation ---
        export_path = None
        if self.skip_conn.isChecked():
            # Smart Naming: Use JSON filename as base
            json_name = os.path.basename(self.json_path)
            base_name = os.path.splitext(json_name)[0]
            default_filename = f"{base_name}.sql"
            
            default_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "exports", default_filename)
            fname, _ = QFileDialog.getSaveFileName(self, "Save SQL Script", default_path, "SQL Files (*.sql)")
            if not fname: return
            export_path = fname
        else:
            # Validation: Check for empty fields before connecting
            missing = []
            if not self.host.text().strip(): missing.append("Host")
            if not self.port.text().strip(): missing.append("Port")
            if not self.db.text().strip(): missing.append("Database")
            if not self.user.text().strip(): missing.append("User")
            
            if missing:
                QMessageBox.warning(self, "Missing Connection Data", 
                                  f"Please fill in the following fields:\n- {', '.join(missing)}\n\n"
                                  "A valid database connection is required.")
                return

            try:
                p = int(self.port.text())
                engine = phoenix_importer.get_engine(self.user.text(), self.pw.text(), self.host.text(), p, self.db.text())
                inspector = sqlalchemy.inspect(engine)
                if inspector.has_table(table_name):
                    logger.info(f"UI: Table '{table_name}' already exists. Asking for confirmation.")
                    msg = f"Table '{table_name}' already exists.\n\nMode: {mode.upper()}\n"
                    if mode == 'nuke': msg += "Warning: This will DELETE all existing data in the table."
                    elif mode == 'upsert': msg += "This will update existing records and add new ones (Recommended)."
                    elif mode == 'append': msg += "This will add new records to the end."
                    
                    res = QMessageBox.question(self, "Confirm Update", msg + "\n\nDo you want to proceed?", 
                                               QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
                    if res == QMessageBox.StandardButton.No:
                        logger.info("UI: User canceled the operation.")
                        return
                else:
                    logger.info(f"UI: Table '{table_name}' does not exist. Generic creation flow.")
            except Exception as e:
                logger.error(f"UI: Error during pre-run check: {e}")
                QMessageBox.warning(self, "Connection Check Failed", f"Could not verify database: {e}\n\nProceeding anyway...")

        params = {
            'json': self.json_path, 'host': self.host.text(), 'port': self.port.text(), 'db': self.db.text(),
            'user': self.user.text(), 'pass': self.pw.text(), 'table': table_name,
            'mode': mode,
            'pk': pk,
            'export_path': export_path
        }
        
        logger.info(f"UI: Final decision: {params['mode']} on '{params['table']}' with PK='{params['pk']}'")
        self.worker = WorkerThread(params)
        self.worker.progress_signal.connect(self.update_log)
        self.worker.finished_signal.connect(lambda s, m: QMessageBox.information(self, "Status", m))
        self.worker.start()

    def update_log(self, msg):
        self.log.append(msg)
        self.log.verticalScrollBar().setValue(self.log.verticalScrollBar().maximum())

if __name__ == "__main__":
    if sys.platform.startswith("win"):
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("nonrealxr.phoenix.sqlimporter")

    app = QApplication(sys.argv)

    icon_path = resource_path("resources/phoenix_icon.ico")
    if os.path.exists(icon_path):
        app.setWindowIcon(QIcon(icon_path))

    window = PhoenixApp()
    window.show()
    sys.exit(app.exec())
