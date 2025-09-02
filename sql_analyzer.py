import tkinter as tk
from tkinter import ttk, filedialog, messagebox, simpledialog
# Use the correct new version not this import google.generativeai as genai
from google import genai
from google.genai import types
import os
import json
import csv
import logging
import queue
import threading
import sqlfluff
import re
import yaml
import teradatasql
from datetime import datetime
import argparse
# Import get_object_ddl for Gemini function calling
from teradata_tools import get_object_ddl, update_db_connection_params
# Import SQL lineage analysis module
from sqllineage import analyze_sql_lineage

# Markdown support
import markdown
from tkinterweb import HtmlFrame

# Check if running in virtual environment
def check_virtual_environment():
    """Check if the application is running in a virtual environment"""
    import sys

    # Check for common virtual environment indicators
    in_venv = (
        hasattr(sys, 'real_prefix') or
        (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix) or
        os.environ.get('VIRTUAL_ENV') is not None or
        os.environ.get('CONDA_DEFAULT_ENV') is not None
    )

    if not in_venv:
        # Show warning but don't prevent running
        root = tk.Tk()
        root.withdraw()  # Hide the main window
        messagebox.showwarning(
            "Virtual Environment Warning",
            "Warning: It appears you're not running in a virtual environment.\n\n"
            "For best results, please use one of the launcher scripts:\n"
            "• run_app.bat (Windows)\n"
            "• run_app.ps1 (PowerShell)\n\n"
            "Or manually activate the virtual environment:\n"
            "venv\\Scripts\\activate\n\n"
            "This ensures all dependencies are properly isolated."
        )
        root.destroy()

# Run environment check
check_virtual_environment()

def parse_arguments():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(description='SQL Lineage Analyzer')
    parser.add_argument('--auto-analyze', action='store_true',
                       help='Automatically analyze the default test.sql file on startup')
    parser.add_argument('--auto-quit', action='store_true',
                       help='Automatically quit after analysis is complete')
    parser.add_argument('--test-mode', action='store_true',
                       help='Run in test mode (non-interactive)')
    return parser.parse_args()

# Parse command-line arguments
args = parse_arguments()

# Function declarations for Gemini API
def create_function_declarations():
    """Create function declarations for Gemini API function calling"""
    return types.Tool(
        function_declarations=[
            types.FunctionDeclaration(
                name='get_object_ddl',
                description='Gets the DDL (Data Definition Language) for a given Teradata object (table or view). Use this function to discover the schema, columns, and types of any object you encounter in the user\'s SQL.',
                parameters=types.Schema(
                    type=types.Type.OBJECT,
                    properties={
                        'table_name': types.Schema(
                            type=types.Type.STRING,
                            description='The fully-qualified name of the table or view (e.g., DATABASE.TABLE_NAME)'
                        )
                    },
                    required=['table_name']
                )
            ),
            types.FunctionDeclaration(
                name='analyze_sql_lineage',
                description='Analyzes SQL content and returns source-to-target data lineage mappings in JSON format. Use this function to extract table relationships, column mappings, and transformation logic from SQL queries.',
                parameters=types.Schema(
                    type=types.Type.OBJECT,
                    properties={
                        'sql_content': types.Schema(
                            type=types.Type.STRING,
                            description='The SQL query content to analyze for data lineage'
                        )
                    },
                    required=['sql_content']
                )
            )
        ]
    )

# --- Config file logic ---
CONFIG_DIR = os.path.join(os.path.expanduser("~"), "sql_an_analizer")
CONFIG_PATH = os.path.join(CONFIG_DIR, "config.yaml")

def load_config():
    if not os.path.exists(CONFIG_PATH):
        return {"host": "", "user": "", "password": "", "logmech": "BROWSER", "api_key": ""}
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f) or {"host": "", "user": "", "password": "", "logmech": "BROWSER", "api_key": ""}

def save_config(config):
    os.makedirs(CONFIG_DIR, exist_ok=True)
    with open(CONFIG_PATH, "w") as f:
        yaml.safe_dump(config, f)

def show_connection_config_dialog(root, config):
    host = simpledialog.askstring("DB Host", "Enter database host:", initialvalue=config.get("host", ""))
    user = simpledialog.askstring("DB User", "Enter username:", initialvalue=config.get("user", ""))
    password = simpledialog.askstring("DB Password", "Enter password:", initialvalue=config.get("password", ""), show="*")
    logmech = simpledialog.askstring("LogMech", "Enter logmech:", initialvalue=config.get("logmech", "BROWSER"))
    config.update({"host": host, "user": user, "password": password, "logmech": logmech})
    save_config(config)
    messagebox.showinfo("Saved", "Connection settings saved.")

# Global DB connection params (shared with teradata_tools)
db_connection_params = load_config()

# Initialize teradata_tools with the connection parameters
update_db_connection_params(db_connection_params)

# --- GUI Classes ---
class QueueHandler(logging.Handler):
    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record):
        self.log_queue.put(self.format(record))

def get_timestamp():
    """Generate timestamp in YYYYMMDDHHMMSS format"""
    return datetime.now().strftime("%Y%m%d%H%M%S")

class SQLAnalyzerApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("SQL Lineage Analyzer")
        self.geometry("1200x800")

        # Delete existing log file on startup
        log_file_path = "analysis.log"
        if os.path.exists(log_file_path):
            try:
                os.remove(log_file_path)
                print(f"Deleted existing log file: {log_file_path}")
            except Exception as e:
                print(f"Warning: Could not delete log file {log_file_path}: {e}")

        # Initialize variables
        self.api_key_var = tk.StringVar(value=db_connection_params.get("api_key", ""))
        self.model_var = tk.StringVar(value="models/gemini-2.0-flash")
        self.output_dir_var = tk.StringVar(value=os.getcwd())
        self.output_format_var = tk.StringVar(value="drawio")
        self.db_host_var = tk.StringVar(value=db_connection_params.get("host", ""))
        self.db_user_var = tk.StringVar(value=db_connection_params.get("user", ""))
        self.db_password_var = tk.StringVar(value=db_connection_params.get("password", ""))
        self.db_logmech_var = tk.StringVar(value=db_connection_params.get("logmech", "BROWSER"))

        # Load prompts
        self.prompts = self._load_prompts()

        # Setup logging
        self.log_queue = queue.Queue()
        self.queue_handler = QueueHandler(self.log_queue)
        logging.basicConfig(level=logging.INFO,
                          format='%(asctime)s - %(levelname)s - %(message)s',
                          handlers=[self.queue_handler])

        # Create a queue for GUI updates from threads
        self.gui_queue = queue.Queue()

        # Initialize function usage tracking
        self.function_usage = {
            'get_object_ddl': 0,
            'analyze_sql_lineage': 0
        }

        # Create GUI
        self._create_widgets()

        # Start log processing and GUI queue
        self.after(100, self._process_log_queue)
        self.after(100, self._process_gui_queue)

        # Check initial connectivity status
        self._check_initial_connectivity()

    def _process_gui_queue(self):
        """Process GUI updates from the queue."""
        try:
            while True:
                callback = self.gui_queue.get_nowait()
                callback()
        except queue.Empty:
            pass
        finally:
            self.after(100, self._process_gui_queue)

    def _queue_gui_update(self, callback):
        """Add a GUI update to the queue."""
        self.gui_queue.put(callback)

    def _load_prompts(self):
        try:
            with open("prompts.yaml", "r") as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            messagebox.showerror("Error", "prompts.yaml file not found!")
            return {"main_analysis": "Analyze this SQL: {sql_content}"}

    def _create_widgets(self):
        # Main container with left panel
        main_container = ttk.Frame(self)
        main_container.pack(fill="both", expand=True, padx=10, pady=10)

        # Left panel for function usage tracking
        left_panel = ttk.LabelFrame(main_container, text="Function Usage", padding="10")
        left_panel.pack(side="left", fill="y", padx=(0, 10))

        # Function usage display
        ttk.Label(left_panel, text="Gemini Function Calls:", font=("Helvetica", 10, "bold")).pack(anchor="w", pady=(0, 10))

        # get_object_ddl usage
        ddl_frame = ttk.Frame(left_panel)
        ddl_frame.pack(fill="x", pady=(0, 5))
        ttk.Label(ddl_frame, text="get_object_ddl:").pack(side="left")
        self.ddl_usage_label = ttk.Label(ddl_frame, text="0", font=("Helvetica", 10, "bold"))
        self.ddl_usage_label.pack(side="right")

        # analyze_sql_lineage usage
        lineage_frame = ttk.Frame(left_panel)
        lineage_frame.pack(fill="x", pady=(0, 5))
        ttk.Label(lineage_frame, text="analyze_sql_lineage:").pack(side="left")
        self.lineage_usage_label = ttk.Label(lineage_frame, text="0", font=("Helvetica", 10, "bold"))
        self.lineage_usage_label.pack(side="right")

        # Reset button
        ttk.Button(left_panel, text="Reset Counters", command=self._reset_usage_counters).pack(fill="x", pady=(10, 0))

        # Right panel for main content
        right_panel = ttk.Frame(main_container)
        right_panel.pack(side="right", fill="both", expand=True)

        # Main frame (moved to right panel)
        main_frame = ttk.Frame(right_panel)
        main_frame.pack(fill="both", expand=True)

        # Configuration frame
        config_frame = ttk.LabelFrame(main_frame, text="Configuration", padding="10")
        config_frame.pack(fill="x", pady=(0, 10))

        # API Key
        ttk.Label(config_frame, text="Gemini API Key:").grid(row=0, column=0, sticky="w", padx=(0, 10))
        ttk.Entry(config_frame, textvariable=self.api_key_var, width=50, show="*").grid(row=0, column=1, sticky="ew", padx=(0, 10))
        ttk.Button(config_frame, text="Fetch Models", command=self._fetch_and_update_models_threaded).grid(row=0, column=2)

        # Model selection
        ttk.Label(config_frame, text="Model:").grid(row=1, column=0, sticky="w", padx=(0, 10), pady=(10, 0))
        self.model_combo = ttk.Combobox(config_frame, textvariable=self.model_var, width=47)
        self.model_combo.grid(row=1, column=1, sticky="ew", padx=(0, 10), pady=(10, 0))

        # Output directory
        ttk.Label(config_frame, text="Output Directory:").grid(row=2, column=0, sticky="w", padx=(0, 10), pady=(10, 0))
        ttk.Entry(config_frame, textvariable=self.output_dir_var).grid(row=2, column=1, sticky="ew", padx=(0, 10), pady=(10, 0))
        ttk.Button(config_frame, text="Browse", command=self._browse_output_dir).grid(row=2, column=2, pady=(10, 0))

        # Output format
        ttk.Label(config_frame, text="Chart Format:").grid(row=3, column=0, sticky="w", padx=(0, 10), pady=(10, 0))
        format_combo = ttk.Combobox(config_frame, textvariable=self.output_format_var,
                                   values=["drawio", "mermaid", "graphviz"])
        format_combo.grid(row=3, column=1, sticky="w", padx=(0, 10), pady=(10, 0))

        # Configure grid weights
        config_frame.columnconfigure(1, weight=1)

        # Database Configuration frame
        db_config_frame = ttk.LabelFrame(main_frame, text="Database Configuration", padding="10")
        db_config_frame.pack(fill="x", pady=(0, 10))

        # Database Host
        ttk.Label(db_config_frame, text="Host:").grid(row=0, column=0, sticky="w", padx=(0, 10))
        ttk.Entry(db_config_frame, textvariable=self.db_host_var, width=50).grid(row=0, column=1, sticky="ew", padx=(0, 10))

        # Logon Mechanism
        ttk.Label(db_config_frame, text="Logon Mechanism:").grid(row=1, column=0, sticky="w", padx=(0, 10), pady=(10, 0))
        logmech_combo = ttk.Combobox(db_config_frame, textvariable=self.db_logmech_var,
                                    values=["BROWSER", "LDAP", "TDNEGO", "KERBEROS", "SPNEGO", "TD2"])
        logmech_combo.grid(row=1, column=1, sticky="w", padx=(0, 10), pady=(10, 0))
        logmech_combo.bind("<<ComboboxSelected>>", self._on_logmech_change)
        logmech_combo.bind("<FocusOut>", self._on_logmech_change)

        # Username
        ttk.Label(db_config_frame, text="Username:").grid(row=2, column=0, sticky="w", padx=(0, 10), pady=(10, 0))
        self.username_entry = ttk.Entry(db_config_frame, textvariable=self.db_user_var, width=50)
        self.username_entry.grid(row=2, column=1, sticky="ew", padx=(0, 10), pady=(10, 0))

        # Password
        ttk.Label(db_config_frame, text="Password:").grid(row=3, column=0, sticky="w", padx=(0, 10), pady=(10, 0))
        self.password_entry = ttk.Entry(db_config_frame, textvariable=self.db_password_var, width=50, show="*")
        self.password_entry.grid(row=3, column=1, sticky="ew", padx=(0, 10), pady=(10, 0))

        # Save DB Config button
        ttk.Button(db_config_frame, text="Save Configuration", command=self._save_db_config).grid(row=4, column=1, sticky="e", pady=(10, 0))

        # Configure grid weights
        db_config_frame.columnconfigure(1, weight=1)

        # Initialize username/password field states
        self._update_credential_fields()

        # Connectivity frame
        connectivity_frame = ttk.LabelFrame(main_frame, text="Connectivity Status", padding="10")
        connectivity_frame.pack(fill="x", pady=(0, 10))

        # Status indicators frame
        status_frame = ttk.Frame(connectivity_frame)
        status_frame.pack(fill="x", pady=(0, 10))

        # AI Status
        ttk.Label(status_frame, text="AI:").grid(row=0, column=0, sticky="w", padx=(0, 5))
        self.ai_status_canvas = tk.Canvas(status_frame, width=20, height=20, bg="white", highlightthickness=1, highlightbackground="gray")
        self.ai_status_canvas.grid(row=0, column=1, padx=(0, 20))
        self.ai_status_label = ttk.Label(status_frame, text="Unknown")
        self.ai_status_label.grid(row=0, column=2, sticky="w", padx=(0, 20))

        # Refresh Status Button
        ttk.Button(status_frame, text="Refresh Status", command=self._refresh_connectivity).grid(row=0, column=6, padx=(10, 0))
        # Database Status
        ttk.Label(status_frame, text="Database:").grid(row=0, column=3, sticky="w", padx=(0, 5))
        self.db_status_canvas = tk.Canvas(status_frame, width=20, height=20, bg="white", highlightthickness=1, highlightbackground="gray")
        self.db_status_canvas.grid(row=0, column=4, padx=(0, 20))
        self.db_status_label = ttk.Label(status_frame, text="Unknown")
        self.db_status_label.grid(row=0, column=5, sticky="w")

        # Connect button
        self.connect_button = ttk.Button(connectivity_frame, text="Connect to Teradata", command=self._connect_to_teradata)
        self.connect_button.pack(side="right")

        # Interactions notebook for separate Database and AI logs
        ttk.Label(connectivity_frame, text="System Interactions:").pack(anchor="w", pady=(10, 0))
        interactions_notebook = ttk.Notebook(connectivity_frame)
        interactions_notebook.pack(fill="both", expand=True, pady=(5, 0))

        # Database Interactions Tab
        db_interactions_frame = ttk.Frame(interactions_notebook)
        interactions_notebook.add(db_interactions_frame, text="Database")

        self.db_interactions_text = tk.Text(db_interactions_frame, height=6, wrap="word", state="disabled")
        db_scrollbar = ttk.Scrollbar(db_interactions_frame, orient="vertical", command=self.db_interactions_text.yview)
        self.db_interactions_text.configure(yscrollcommand=db_scrollbar.set)

        self.db_interactions_text.pack(side="left", fill="both", expand=True)
        db_scrollbar.pack(side="right", fill="y")

        # AI Interactions Tab
        ai_interactions_frame = ttk.Frame(interactions_notebook)
        interactions_notebook.add(ai_interactions_frame, text="AI")

        self.ai_interactions_text = tk.Text(ai_interactions_frame, height=6, wrap="word", state="disabled")
        ai_scrollbar = ttk.Scrollbar(ai_interactions_frame, orient="vertical", command=self.ai_interactions_text.yview)
        self.ai_interactions_text.configure(yscrollcommand=ai_scrollbar.set)

        self.ai_interactions_text.pack(side="left", fill="both", expand=True)
        ai_scrollbar.pack(side="right", fill="y")

        # Clear buttons frame
        clear_buttons_frame = ttk.Frame(connectivity_frame)
        clear_buttons_frame.pack(fill="x", pady=(5, 0))

        ttk.Button(clear_buttons_frame, text="Clear DB Log", command=self._clear_db_interactions_log).pack(side="left", padx=(0, 10))
        ttk.Button(clear_buttons_frame, text="Clear AI Log", command=self._clear_ai_interactions_log).pack(side="left")

        # Initialize status indicators
        self._update_status_indicators()

        # Action frame = ttk.Frame(main_frame)
        # action_frame.pack(fill="x", pady=(0, 10))

        # File selection
        ttk.Label(main_frame, text="SQL Files:").pack(side="left", padx=(0, 10))
        self.file_listbox = tk.Listbox(main_frame, height=1, width=50)
        self.file_listbox.pack(side="left", fill="x", expand=True, padx=(0, 10))
        ttk.Button(main_frame, text="Browse Files", command=self._browse_files).pack(side="left", padx=(0, 10))
        ttk.Button(main_frame, text="Clear Files", command=self._clear_files).pack(side="left", padx=(0, 10))

        # Load default test.sql file if it exists
        self._load_default_file()

        # Run button
        self.run_button = ttk.Button(main_frame, text="Run Analysis", command=self.run_analysis_threaded)
        self.run_button.pack(side="right")

        # Results notebook
        self.results_notebook = ttk.Notebook(main_frame)
        self.results_notebook.pack(fill="both", expand=True)

        # Bind tab change event to refresh displays
        self.results_notebook.bind("<<NotebookTabChanged>>", self._on_tab_changed)

        # Object Lineage Tab
        obj_frame = ttk.Frame(self.results_notebook)
        self.results_notebook.add(obj_frame, text="Object Lineage")

        ttk.Label(obj_frame, text="Object-to-Object Lineage", font=("Helvetica", 10, "bold")).pack(pady=(10, 5))

        obj_tree_frame = ttk.Frame(obj_frame)
        obj_tree_frame.pack(fill="both", expand=True, padx=10, pady=10)

        self.obj_lineage_tree = ttk.Treeview(obj_tree_frame, columns=("Source Table", "Target Table"), show="headings", height=8)
        self.obj_lineage_tree.heading("Source Table", text="Source Table")
        self.obj_lineage_tree.heading("Target Table", text="Target Table")
        self.obj_lineage_tree.column("Source Table", width=300)
        self.obj_lineage_tree.column("Target Table", width=300)

        obj_scrollbar_y = ttk.Scrollbar(obj_tree_frame, orient="vertical", command=self.obj_lineage_tree.yview)
        obj_scrollbar_x = ttk.Scrollbar(obj_tree_frame, orient="horizontal", command=self.obj_lineage_tree.xview)
        self.obj_lineage_tree.configure(yscrollcommand=obj_scrollbar_y.set, xscrollcommand=obj_scrollbar_x.set)

        self.obj_lineage_tree.pack(side="left", fill="both", expand=True)
        obj_scrollbar_y.pack(side="right", fill="y")
        obj_scrollbar_x.pack(side="bottom", fill="x")

        # Column Lineage Tab
        col_frame = ttk.Frame(self.results_notebook)
        self.results_notebook.add(col_frame, text="Column Lineage")

        ttk.Label(col_frame, text="Column-to-Column Lineage", font=("Helvetica", 10, "bold")).pack(pady=(10, 5))

        col_tree_frame = ttk.Frame(col_frame)
        col_tree_frame.pack(fill="both", expand=True, padx=10, pady=10)

        self.col_lineage_tree = ttk.Treeview(col_tree_frame, columns=("Source Table", "Source Column", "Target Table", "Target Column"), show="headings", height=8)
        self.col_lineage_tree.heading("Source Table", text="Source Table")
        self.col_lineage_tree.heading("Source Column", text="Source Column")
        self.col_lineage_tree.heading("Target Table", text="Target Table")
        self.col_lineage_tree.heading("Target Column", text="Target Column")
        self.col_lineage_tree.column("Source Table", width=200)
        self.col_lineage_tree.column("Source Column", width=150)
        self.col_lineage_tree.column("Target Table", width=200)
        self.col_lineage_tree.column("Target Column", width=150)

        col_scrollbar_y = ttk.Scrollbar(col_tree_frame, orient="vertical", command=self.col_lineage_tree.yview)
        col_scrollbar_x = ttk.Scrollbar(col_tree_frame, orient="horizontal", command=self.col_lineage_tree.xview)
        self.col_lineage_tree.configure(yscrollcommand=col_scrollbar_y.set, xscrollcommand=col_scrollbar_x.set)

        self.col_lineage_tree.pack(side="left", fill="both", expand=True)
        col_scrollbar_y.pack(side="right", fill="y")
        col_scrollbar_x.pack(side="bottom", fill="x")

        # Logs Tab
        log_frame = ttk.Frame(self.results_notebook)
        self.results_notebook.add(log_frame, text="Logs")

        self.log_text = tk.Text(log_frame, height=10, state="disabled")
        self.log_text.pack(fill="both", expand=True, padx=10, pady=10)

        log_scrollbar = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview)
        log_scrollbar.pack(side="right", fill="y")
        self.log_text.config(yscrollcommand=log_scrollbar.set)

        # Chat Tab
        chat_frame = ttk.Frame(self.results_notebook)
        self.results_notebook.add(chat_frame, text="Chat")

        # Chat display with markdown support
        chat_paned = ttk.PanedWindow(chat_frame, orient="horizontal")
        chat_paned.pack(fill="both", expand=True, padx=10, pady=(10, 5))

        # Chat history display (left side)
        chat_left_frame = ttk.Frame(chat_paned)
        chat_paned.add(chat_left_frame, weight=1)

        ttk.Label(chat_left_frame, text="Chat History", font=("Helvetica", 10, "bold")).pack(pady=(0, 5))

        self.chat_text = tk.Text(chat_left_frame, height=15, wrap="word", state="disabled")
        chat_scrollbar = ttk.Scrollbar(chat_left_frame, orient="vertical", command=self.chat_text.yview)
        self.chat_text.configure(yscrollcommand=chat_scrollbar.set)

        self.chat_text.pack(side="left", fill="both", expand=True)
        chat_scrollbar.pack(side="right", fill="y")

        # Markdown display (right side)
        chat_right_frame = ttk.Frame(chat_paned)
        chat_paned.add(chat_right_frame, weight=1)

        ttk.Label(chat_right_frame, text="Markdown Preview", font=("Helvetica", 10, "bold")).pack(pady=(0, 5))

        # Markdown display frame
        self.markdown_frame = ttk.Frame(chat_right_frame)
        self.markdown_frame.pack(fill="both", expand=True)

        # Initialize markdown display
        self._setup_markdown_display()

        # Chat input area
        chat_input_frame = ttk.Frame(chat_frame)
        chat_input_frame.pack(fill="x", padx=10, pady=(5, 10))

        self.chat_input = tk.Text(chat_input_frame, height=3, wrap="word")
        self.chat_input.pack(side="left", fill="x", expand=True)

        # Chat buttons
        chat_buttons_frame = ttk.Frame(chat_input_frame)
        chat_buttons_frame.pack(side="right", padx=(10, 0))

        ttk.Button(chat_buttons_frame, text="Send", command=self._send_chat_message).pack(side="top", pady=(0, 5))
        ttk.Button(chat_buttons_frame, text="Clear", command=self._clear_chat_history).pack(side="top")

        # Bind Enter key to send message
        self.chat_input.bind("<Return>", lambda e: self._send_chat_message() if not e.state & 0x1 else None)  # Shift+Enter for new line
        self.chat_input.bind("<Shift-Return>", lambda e: self.chat_input.insert("end", "\n"))

        # Initialize chat conversation history
        self.chat_history = []

        # Add welcome message to chat
        self._initialize_chat()

        # Explanations Tab
        explanations_frame = ttk.Frame(self.results_notebook)
        self.results_notebook.add(explanations_frame, text="Explanations")

        # Explanations display
        explanations_display_frame = ttk.Frame(explanations_frame)
        explanations_display_frame.pack(fill="both", expand=True, padx=10, pady=(10, 5))

        ttk.Label(explanations_display_frame, text="Saved Explanations", font=("Helvetica", 10, "bold")).pack(pady=(0, 5))

        # Explanations listbox and display
        explanations_paned = ttk.PanedWindow(explanations_display_frame, orient="horizontal")
        explanations_paned.pack(fill="both", expand=True)

        # File list (left side)
        explanations_left_frame = ttk.Frame(explanations_paned)
        explanations_paned.add(explanations_left_frame, weight=1)

        ttk.Label(explanations_left_frame, text="Explanation Files", font=("Helvetica", 9, "bold")).pack(pady=(0, 5))

        self.explanations_listbox = tk.Listbox(explanations_left_frame, height=10)
        explanations_list_scrollbar = ttk.Scrollbar(explanations_left_frame, orient="vertical", command=self.explanations_listbox.yview)
        self.explanations_listbox.configure(yscrollcommand=explanations_list_scrollbar.set)

        self.explanations_listbox.pack(side="left", fill="both", expand=True)
        explanations_list_scrollbar.pack(side="right", fill="y")

        # Bind listbox selection
        self.explanations_listbox.bind("<<ListboxSelect>>", self._on_explanation_select)

        # Markdown display (right side)
        explanations_right_frame = ttk.Frame(explanations_paned)
        explanations_paned.add(explanations_right_frame, weight=1)

        ttk.Label(explanations_right_frame, text="Content Preview", font=("Helvetica", 9, "bold")).pack(pady=(0, 5))

        self.explanations_markdown_frame = ttk.Frame(explanations_right_frame)
        self.explanations_markdown_frame.pack(fill="both", expand=True)

        # Setup explanations markdown display
        self._setup_explanations_markdown_display()

        # Explanations buttons
        explanations_buttons_frame = ttk.Frame(explanations_frame)
        explanations_buttons_frame.pack(fill="x", padx=10, pady=(5, 10))

        ttk.Button(explanations_buttons_frame, text="Refresh List", command=self._refresh_explanations_list).pack(side="left", padx=(0, 10))
        ttk.Button(explanations_buttons_frame, text="Open File", command=self._open_explanation_file).pack(side="left", padx=(0, 10))
        ttk.Button(explanations_buttons_frame, text="Delete File", command=self._delete_explanation_file).pack(side="left")

        # Initialize explanations list
        self._refresh_explanations_list()

    def _reset_usage_counters(self):
        """Reset all function usage counters to zero"""
        self.function_usage['get_object_ddl'] = 0
        self.function_usage['analyze_sql_lineage'] = 0
        self._update_usage_display()

    def _update_usage_display(self):
        """Update the usage display labels"""
        self.ddl_usage_label.config(text=str(self.function_usage['get_object_ddl']))
        self.lineage_usage_label.config(text=str(self.function_usage['analyze_sql_lineage']))

    def _increment_usage_counter(self, function_name):
        """Increment the usage counter for a specific function"""
        if function_name in self.function_usage:
            self.function_usage[function_name] += 1
            self._update_usage_display()

    def _setup_markdown_display(self):
        """Setup markdown display widget"""
        try:
            self.markdown_html = HtmlFrame(self.markdown_frame, horizontal_scrollbar="auto")
            self.markdown_html.pack(fill="both", expand=True)
            self.markdown_html.load_html("<h3>Markdown Preview</h3><p>Select a message to preview markdown content.</p>")
        except Exception as e:
            # Fallback to text widget if HtmlFrame fails
            self.markdown_text = tk.Text(self.markdown_frame, height=15, wrap="word", state="disabled")
            markdown_scrollbar = ttk.Scrollbar(self.markdown_frame, orient="vertical", command=self.markdown_text.yview)
            self.markdown_text.configure(yscrollcommand=markdown_scrollbar.set)
            self.markdown_text.pack(side="left", fill="both", expand=True)
            markdown_scrollbar.pack(side="right", fill="y")
            self.markdown_text.insert("1.0", "Markdown Preview\n\nSelect a message to preview markdown content.")
            self.markdown_text.config(state="disabled")

    def _update_markdown_display(self, content):
        """Update markdown display with new content"""
        try:
            if hasattr(self, 'markdown_html'):
                # Convert markdown to HTML
                html_content = markdown.markdown(content, extensions=['tables', 'fenced_code', 'codehilite'])
                # Add basic styling
                styled_html = f"""
                <html>
                <head>
                    <style>
                        body {{ font-family: Arial, sans-serif; margin: 10px; }}
                        h1, h2, h3 {{ color: #2E86C1; }}
                        code {{ background-color: #f4f4f4; padding: 2px 4px; border-radius: 3px; }}
                        pre {{ background-color: #f4f4f4; padding: 10px; border-radius: 5px; overflow-x: auto; }}
                        table {{ border-collapse: collapse; width: 100%; }}
                        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                        th {{ background-color: #f2f2f2; }}
                    </style>
                </head>
                <body>
                    {html_content}
                </body>
                </html>
                """
                self.markdown_html.load_html(styled_html)
            elif hasattr(self, 'markdown_text'):
                self.markdown_text.config(state="normal")
                self.markdown_text.delete("1.0", "end")
                self.markdown_text.insert("1.0", content)
                self.markdown_text.config(state="disabled")
        except Exception as e:
            print(f"Error updating markdown display: {e}")

    def _save_markdown_explanation(self, content, filename=None):
        """Save markdown content to file"""
        if not filename:
            timestamp = get_timestamp()
            filename = f"explanation_{timestamp}.md"
        
        filepath = os.path.join(os.getcwd(), "explanations", filename)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return filepath

    def _handle_function_calls(self, response, client, function_declarations):
        """Handle function calls in Gemini response"""
        try:
            # Check for function calls in different possible locations
            function_calls = None
            
            # Try different ways to access function calls
            if hasattr(response, 'function_calls') and response.function_calls:
                function_calls = response.function_calls
            elif hasattr(response, 'function_call') and response.function_call:
                function_calls = [response.function_call]
            elif hasattr(response, 'candidates') and response.candidates:
                for candidate in response.candidates:
                    if hasattr(candidate, 'function_calls') and candidate.function_calls:
                        function_calls = candidate.function_calls
                        break
                    elif hasattr(candidate, 'function_call') and candidate.function_call:
                        function_calls = [candidate.function_call]
                        break
            
            if function_calls:
                # Execute function calls
                function_results = []
                for function_call in function_calls:
                    result = self._execute_function_call(function_call)
                    function_results.append(result)
                
                # Send function results back to Gemini
                if function_results:
                    try:
                        follow_up_response = client.models.generate_content(
                            model=self.model_var.get(),
                            contents=[
                                types.Content(
                                    role="model",
                                    parts=[types.Part(text="Function calls executed.")]
                                ),
                                types.Content(
                                    role="user", 
                                    parts=[types.Part(text=f"Function results: {function_results}")]
                                )
                            ]
                        )
                        return follow_up_response.text
                    except Exception as e:
                        logging.error(f"Error sending follow-up response: {e}")
                        return f"Function calls executed but error sending results: {function_results}"
            
            # No function calls, return the text response
            return response.text
            
        except Exception as e:
            logging.error(f"Error handling function calls: {e}")
            # Fallback to returning the text response
            try:
                return response.text
            except:
                return f"Error processing response: {e}"

    def _execute_function_call(self, function_call):
        """Execute a function call from Gemini"""
        try:
            # Handle different function call structures
            func_name = None
            func_args = None
            
            if hasattr(function_call, 'name'):
                func_name = function_call.name
            elif hasattr(function_call, 'function') and hasattr(function_call.function, 'name'):
                func_name = function_call.function.name
            
            if hasattr(function_call, 'args'):
                func_args = function_call.args
            elif hasattr(function_call, 'function') and hasattr(function_call.function, 'args'):
                func_args = function_call.function.args
            
            if not func_name:
                return "Error: Function call missing name"
            
            if func_name == "get_object_ddl":
                # Increment usage counter
                self._increment_usage_counter('get_object_ddl')
                
                if func_args and hasattr(func_args, 'get'):
                    table_name = func_args.get("table_name")
                elif isinstance(func_args, dict):
                    table_name = func_args.get("table_name")
                else:
                    table_name = None
                
                if table_name:
                    from teradata_tools import get_object_ddl
                    result = get_object_ddl(table_name)
                    return f"DDL for {table_name}: {result}"
                else:
                    return "Error: No table_name provided in function call"
            elif func_name == "analyze_sql_lineage":
                # Increment usage counter
                self._increment_usage_counter('analyze_sql_lineage')
                
                if func_args and hasattr(func_args, 'get'):
                    sql_content = func_args.get("sql_content")
                elif isinstance(func_args, dict):
                    sql_content = func_args.get("sql_content")
                else:
                    sql_content = None
                
                if sql_content:
                    from sqllineage import analyze_sql_lineage
                    result = analyze_sql_lineage(sql_content)
                    return f"SQL Lineage Analysis: {json.dumps(result, indent=2)}"
                else:
                    return "Error: No sql_content provided in function call"
            else:
                return f"Error: Unknown function {func_name}"
                
        except Exception as e:
            logging.error(f"Error executing function call: {e}")
            return f"Error executing function call: {e}"

    def _setup_explanations_markdown_display(self):
        """Setup markdown display for explanations tab"""
        try:
            self.explanations_markdown_html = HtmlFrame(self.explanations_markdown_frame, horizontal_scrollbar="auto")
            self.explanations_markdown_html.pack(fill="both", expand=True)
            self.explanations_markdown_html.load_html("<h3>Explanation Preview</h3><p>Select a file from the list to preview its content.</p>")
        except Exception as e:
            # Fallback to text widget if HtmlFrame fails
            self.explanations_markdown_text = tk.Text(self.explanations_markdown_frame, height=15, wrap="word", state="disabled")
            explanations_markdown_scrollbar = ttk.Scrollbar(self.explanations_markdown_frame, orient="vertical", command=self.explanations_markdown_text.yview)
            self.explanations_markdown_text.configure(yscrollcommand=explanations_markdown_scrollbar.set)
            self.explanations_markdown_text.pack(side="left", fill="both", expand=True)
            explanations_markdown_scrollbar.pack(side="right", fill="y")
            self.explanations_markdown_text.insert("1.0", "Explanation Preview\n\nSelect a file from the list to preview its content.")
            self.explanations_markdown_text.config(state="disabled")

    def _refresh_explanations_list(self):
        """Refresh the list of explanation files"""
        self.explanations_listbox.delete(0, tk.END)
        explanations_dir = os.path.join(os.getcwd(), "explanations")
        
        if os.path.exists(explanations_dir):
            for filename in sorted(os.listdir(explanations_dir)):
                if filename.endswith('.md'):
                    self.explanations_listbox.insert(tk.END, filename)

    def _on_explanation_select(self, event):
        """Handle explanation file selection"""
        selection = self.explanations_listbox.curselection()
        if selection:
            filename = self.explanations_listbox.get(selection[0])
            self._load_explanation_file(filename)

    def _load_explanation_file(self, filename):
        """Load and display explanation file content"""
        filepath = os.path.join(os.getcwd(), "explanations", filename)
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
            
            self._update_explanations_markdown_display(content)
        except Exception as e:
            error_msg = f"Error loading file: {e}"
            self._update_explanations_markdown_display(f"# Error\n\n{error_msg}")

    def _update_explanations_markdown_display(self, content):
        """Update explanations markdown display with new content"""
        try:
            if hasattr(self, 'explanations_markdown_html'):
                # Convert markdown to HTML
                html_content = markdown.markdown(content, extensions=['tables', 'fenced_code', 'codehilite'])
                # Add basic styling
                styled_html = f"""
                <html>
                <head>
                    <style>
                        body {{ font-family: Arial, sans-serif; margin: 10px; }}
                        h1, h2, h3 {{ color: #2E86C1; }}
                        code {{ background-color: #f4f4f4; padding: 2px 4px; border-radius: 3px; }}
                        pre {{ background-color: #f4f4f4; padding: 10px; border-radius: 5px; overflow-x: auto; }}
                        table {{ border-collapse: collapse; width: 100%; }}
                        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                        th {{ background-color: #f2f2f2; }}
                    </style>
                </head>
                <body>
                    {html_content}
                </body>
                </html>
                """
                self.explanations_markdown_html.load_html(styled_html)
            elif hasattr(self, 'explanations_markdown_text'):
                self.explanations_markdown_text.config(state="normal")
                self.explanations_markdown_text.delete("1.0", "end")
                self.explanations_markdown_text.insert("1.0", content)
                self.explanations_markdown_text.config(state="disabled")
        except Exception as e:
            print(f"Error updating explanations markdown display: {e}")

    def _open_explanation_file(self):
        """Open selected explanation file in default editor"""
        selection = self.explanations_listbox.curselection()
        if selection:
            filename = self.explanations_listbox.get(selection[0])
            filepath = os.path.join(os.getcwd(), "explanations", filename)
            
            try:
                os.startfile(filepath)  # Windows
            except AttributeError:
                # For non-Windows systems
                import subprocess
                subprocess.run(['xdg-open', filepath])  # Linux
            except Exception as e:
                messagebox.showerror("Error", f"Could not open file: {e}")

    def _delete_explanation_file(self):
        """Delete selected explanation file"""
        selection = self.explanations_listbox.curselection()
        if selection:
            filename = self.explanations_listbox.get(selection[0])
            filepath = os.path.join(os.getcwd(), "explanations", filename)
            
            if messagebox.askyesno("Confirm Delete", f"Are you sure you want to delete '{filename}'?"):
                try:
                    os.remove(filepath)
                    self._refresh_explanations_list()
                    # Clear display
                    self._update_explanations_markdown_display("# File Deleted\n\nThe explanation file has been deleted.")
                except Exception as e:
                    messagebox.showerror("Error", f"Could not delete file: {e}")

    def _save_analysis_markdown_explanation(self, response_text, mappings, sql_content, output_dir):
        """Save comprehensive analysis explanation as markdown"""
        timestamp = get_timestamp()
        
        # Create markdown content
        markdown_content = f"""# SQL Lineage Analysis Report
**Analysis Date:** {timestamp}
**Model Used:** {self.model_var.get()}

## Original SQL Query
```sql
{sql_content}
```

## AI Analysis Summary
{response_text.replace('```json', '').replace('```', '').strip()}

## Data Lineage Mappings

### Object-Level Lineage
| Source Table | Target Table |
|-------------|-------------|
"""
        
        # Add object mappings table
        if mappings:
            for mapping in mappings:
                if not isinstance(mapping, dict):
                    continue
                source = mapping.get('source_table', mapping.get('source', mapping.get('from_table', 'Unknown')))
                target = mapping.get('target_table', mapping.get('target', mapping.get('to_table', 'Unknown')))
                markdown_content += f"| {source} | {target} |\n"
        
        markdown_content += "\n### Column-Level Lineage\n"
        markdown_content += "| Source Table | Source Column | Target Table | Target Column |\n"
        markdown_content += "|-------------|---------------|-------------|---------------|\n"
        
        # Add column mappings table
        if mappings:
            for mapping in mappings:
                if not isinstance(mapping, dict):
                    continue
                source_table = mapping.get('source_table', mapping.get('source', mapping.get('from_table', 'Unknown')))
                source_column = mapping.get('source_column', mapping.get('source_col', mapping.get('from_column', 'Unknown')))
                target_table = mapping.get('target_table', mapping.get('target', mapping.get('to_table', 'Unknown')))
                target_column = mapping.get('target_column', mapping.get('target_col', mapping.get('to_column', 'Unknown')))
                markdown_content += f"| {source_table} | {source_column} | {target_table} | {target_column} |\n"
        
        markdown_content += f"""

## Generated Files
- **Object Lineage CSV:** `{os.path.join(output_dir, 'object_lineage.csv')}`
- **Column Lineage CSV:** `{os.path.join(output_dir, 'column_lineage.csv')}`
- **Lineage Graph JSON:** `{os.path.join(output_dir, 'lineage_graph.json')}`
- **Data Flow Diagram:** `{os.path.join(output_dir, 'data_flow.drawio')}`

## Analysis Configuration
- **Database Host:** {self.db_host_var.get() or 'Not configured'}
- **Logon Mechanism:** {self.db_logmech_var.get() or 'Not configured'}
- **Output Directory:** `{output_dir}`
"""
        
        # Save the markdown file
        filename = f"analysis_explanation_{timestamp}.md"
        filepath = self._save_markdown_explanation(markdown_content, filename)
        
        logging.info(f"Analysis explanation saved as markdown: {filepath}")
        
        # Update the markdown display with the analysis explanation
        self._queue_gui_update(lambda: self._update_markdown_display(markdown_content))

    def _initialize_chat(self):
        """Initialize chat with welcome message"""
        welcome_message = """Welcome to the SQL Analysis Assistant! 🤖

I'm here to help you with:
• SQL query analysis and optimization
• Data lineage mapping
• Database schema exploration
• General SQL and database questions

You can ask me questions like:
• "Analyze this SQL query for me"
• "What does this table contain?"
• "Help me understand this data flow"
• "Show me the schema for table X"

I can also access your Teradata database to get schema information when needed.

How can I help you today?"""
        
        self._add_chat_message("AI", welcome_message)

    def _on_tab_changed(self, event):
        """Handle tab change events"""
        current_tab = self.results_notebook.select()
        tab_text = self.results_notebook.tab(current_tab, "text")
        
        if tab_text == "Chat":
            # Refresh chat display when chat tab is selected
            self._refresh_chat_display()

    def _refresh_chat_display(self):
        """Refresh the chat display with current conversation history"""
        if hasattr(self, 'chat_text'):
            self.chat_text.config(state="normal")
            self.chat_text.delete("1.0", "end")
            
            for role, message in self.chat_history:
                self._add_chat_message(role, message)
            
            self.chat_text.config(state="disabled")
            # Auto-scroll to bottom
            self.chat_text.see("end")

    def _send_chat_message(self):
        """Send a chat message to the AI"""
        message = self.chat_input.get("1.0", "end-1c").strip()
        if not message:
            return

        # Add user message to chat
        self._add_chat_message("You", message)

        # Clear input
        self.chat_input.delete("1.0", "end")

        # Send to AI in background thread
        thread = threading.Thread(target=self._process_chat_message, args=(message,))
        thread.daemon = True
        thread.start()

    def _process_chat_message(self, user_message):
        """Process chat message with AI in background thread"""
        try:
            api_key = self.api_key_var.get().strip()
            if not api_key:
                self._add_chat_message("AI", "Error: Please set your Gemini API key first.")
                return

            # Create AI client
            client = genai.Client(api_key=api_key)

            # Prepare conversation context
            conversation_context = ""
            if self.chat_history:
                # Include recent conversation history (last 10 messages)
                recent_history = self.chat_history[-10:]
                conversation_context = "\n".join([f"{role}: {msg}" for role, msg in recent_history])
                conversation_context = f"Previous conversation:\n{conversation_context}\n\n"

            # Load chat prompt from prompts.yaml
            chat_prompt_template = self.prompts.get('chat_assistant', 
                "You are a helpful SQL analysis assistant. Respond to: {user_message}")
            
            # Create chat prompt
            chat_prompt = chat_prompt_template.format(user_message=user_message)
            
            # Add conversation context
            if conversation_context:
                chat_prompt = f"{conversation_context}\n\n{chat_prompt}"

            # Add database context if available
            if db_connection_params.get("host"):
                chat_prompt += f"\n\nDatabase connection available: {db_connection_params['host']}"
                # Add function calling capability for database queries
                function_declarations = create_function_declarations()
                config = types.GenerateContentConfig(tools=function_declarations)
            else:
                config = None
                function_declarations = None

            # Send to AI
            if function_declarations:
                response = client.models.generate_content(
                    model=self.model_var.get(),
                    contents=chat_prompt,
                    config=config
                )
            else:
                response = client.models.generate_content(
                    model=self.model_var.get(),
                    contents=chat_prompt
                )

            # Handle function calls if any
            ai_response = self._handle_function_calls(response, client, function_declarations)

            # Save explanation as markdown file
            timestamp = get_timestamp()
            markdown_filename = f"chat_explanation_{timestamp}.md"
            
            # Format as markdown
            markdown_content = f"""# SQL Analysis Explanation
**Timestamp:** {timestamp}

## User Query
{user_message}

## AI Response
{ai_response}

## Context
- Model: {self.model_var.get()}
- Database: {db_connection_params.get('host', 'Not connected')}
"""

            # Save to file
            saved_filepath = self._save_markdown_explanation(markdown_content, markdown_filename)
            
            # Add AI response to chat with file reference
            response_with_file = f"{ai_response}\n\n*Explanation saved to: {saved_filepath}*"
            self._queue_gui_update(lambda: self._add_chat_message("AI", response_with_file))

            # Update markdown display with the new content
            self._queue_gui_update(lambda: self._update_markdown_display(markdown_content))

            # Log the interaction
            self._queue_gui_update(lambda: self._log_ai_interaction(f"Chat: {user_message[:50]}... (saved as {markdown_filename})"))

        except Exception as e:
            error_msg = f"Error communicating with AI: {str(e)}"
            self._queue_gui_update(lambda: self._add_chat_message("AI", error_msg))
            self._queue_gui_update(lambda: self._log_ai_interaction(f"Chat error: {str(e)}"))

    def _add_chat_message(self, sender, message):
        """Add a message to the chat display"""
        timestamp = get_timestamp()

        # Format message
        formatted_message = f"[{timestamp}] {sender}: {message}\n\n"

        # Add to display
        self.chat_text.config(state="normal")
        self.chat_text.insert("end", formatted_message)
        self.chat_text.see("end")
        self.chat_text.config(state="disabled")

        # Add to history
        self.chat_history.append((sender, message))

    def _clear_chat_history(self):
        """Clear the chat history"""
        self.chat_text.config(state="normal")
        self.chat_text.delete("1.0", "end")
        self.chat_text.config(state="disabled")
        self.chat_history = []
        self._log_ai_interaction("Chat history cleared")

    def _browse_files(self):
        files = filedialog.askopenfilenames(filetypes=[("SQL files", "*.sql"), ("All files", "*.*")])
        if files:
            self.file_listbox.delete(0, tk.END)
            for file in files:
                self.file_listbox.insert(tk.END, file)

    def _clear_files(self):
        self.file_listbox.delete(0, tk.END)

    def _browse_output_dir(self):
        dir_path = filedialog.askdirectory()
        if dir_path:
            self.output_dir_var.set(dir_path)

    def run_analysis_threaded(self):
        # Get selected files
        files = list(self.file_listbox.get(0, tk.END))
        if not files:
            if not args.auto_quit:
                messagebox.showwarning("No Files", "Please select at least one SQL file to analyze.")
            else:
                logging.error("No files selected for analysis in auto mode")
            return

        # Check API key
        api_key = self.api_key_var.get()
        if not api_key:
            if not args.auto_quit:
                messagebox.showerror("No API Key", "Please enter your Gemini API Key.")
            else:
                logging.error("No API key configured in auto mode")
            return

        # Disable run button
        self.run_button.config(state="disabled")

        # Clear previous results
        self._clear_lineage_displays()

        # Start analysis in background thread
        thread = threading.Thread(target=self._run_analysis, args=(files, api_key))
        thread.daemon = True
        thread.start()

    def _run_analysis(self, files, api_key):
        try:
            # Process each file
            for sql_file in files:
                with open(sql_file, 'r', encoding='utf-8') as f:
                    sql_content = f.read()

                # Format SQL
                logging.info("Formatting SQL with sqlfluff...")
                try:
                    formatted_sql = sqlfluff.fix(sql_content, dialect="teradata")
                    logging.info("SQL formatting successful.")
                except Exception as e:
                    logging.warning(f"SQLFluff formatting failed: {e}. Proceeding with raw SQL.")
                    formatted_sql = sql_content

                # Configure the client with API key
                client = genai.Client(api_key=api_key)

                # Check if we should use DB tools
                use_db = bool(self.db_host_var.get().strip())
                tools = None
                if use_db:
                    tools = [create_function_declarations()]
                    db_connection_params["host"] = self.db_host_var.get()
                    db_connection_params["user"] = self.db_user_var.get()
                    db_connection_params["password"] = self.db_password_var.get()
                    db_connection_params["logmech"] = self.db_logmech_var.get()
                    logging.info(f"Database tools enabled for host: {db_connection_params['host']}")
                    
                    # Update teradata_tools with the connection parameters
                    update_db_connection_params(db_connection_params)

                prompt = self.prompts['main_analysis'].format(sql_content=formatted_sql)

                # Generate content using the new google.genai API
                if use_db and tools:
                    response = client.models.generate_content(
                        model=self.model_var.get(),
                        contents=prompt,
                        config=types.GenerateContentConfig(
                            tools=tools
                        )
                    )
                else:
                    response = client.models.generate_content(
                        model=self.model_var.get(),
                        contents=prompt
                    )

                # Handle function calls if any
                final_response_text = self._handle_function_calls(response, client, tools)
                output_dir = self.output_dir_var.get()
                self._process_and_save_output(final_response_text, output_dir, sql_content)

            # Show success message only if not in auto-quit mode
            if not args.auto_quit:
                messagebox.showinfo("Success", "Analysis complete! Output files have been saved.")
            else:
                logging.info("Analysis complete! Output files have been saved.")

            # Auto-quit if requested
            if args.auto_quit:
                logging.info("Auto-quit enabled, closing application...")
                self.after(1000, self.quit)  # Give user 1 second to see the success message

        except Exception as e:
            logging.error(f"An error occurred: {e}", exc_info=True)
            
            # Show error message only if not in auto-quit mode
            if not args.auto_quit:
                messagebox.showerror("Error", f"An error occurred during analysis:\n{e}")

            # Auto-quit on error if requested
            if args.auto_quit:
                logging.info("Auto-quit enabled, closing application after error...")
                self.after(2000, self.quit)  # Give user 2 seconds to see the error message

        finally:
            # Re-enable run button
            self.run_button.config(state="normal")

    def _auto_analyze_default_file(self):
        """Automatically analyze the default test.sql file for testing purposes"""
        logging.info("Starting automatic analysis of default file...")

        # Check if default file exists
        default_file = "test.sql"
        if not os.path.exists(default_file):
            logging.warning(f"Default file {default_file} not found, skipping auto-analysis")
            return

        # Check API key
        api_key = self.api_key_var.get()
        if not api_key:
            logging.warning("No API key configured, skipping auto-analysis")
            return

        # Check if AI connection is established
        ai_status = self.ai_status_label.cget("text")
        if ai_status not in ["Connected", "Connection Failed"]:
            logging.info("Waiting for AI connectivity check to complete...")
            # Retry after another 2 seconds
            self.after(2000, self._auto_analyze_default_file)
            return
        elif ai_status == "Connection Failed":
            logging.warning("AI connection failed, but proceeding with analysis anyway")

        # Check database connection if configured
        db_host = self.db_host_var.get().strip()
        if db_host:
            db_status = self.db_status_label.cget("text")
            if db_status not in ["Connected", "Connection Failed", "Not Connected"]:
                logging.info("Waiting for database connectivity check to complete...")
                # Retry after another 2 seconds
                self.after(2000, self._auto_analyze_default_file)
                return
            elif db_status == "Connection Failed":
                logging.warning("Database connection failed, proceeding without database tools")

        # Load the default file into the listbox if not already loaded
        current_files = list(self.file_listbox.get(0, tk.END))
        if default_file not in current_files:
            self.file_listbox.insert(tk.END, default_file)
            logging.info(f"Loaded default file for auto-analysis: {default_file}")

        # Disable run button
        self.run_button.config(state="disabled")

        # Clear previous results
        self._clear_lineage_displays()

        # Start analysis in background thread
        thread = threading.Thread(target=self._run_analysis, args=([default_file], api_key))
        thread.daemon = True
        thread.start()

        logging.info("Auto-analysis started successfully")

    def _clear_lineage_displays(self):
        """Clear the lineage display tables"""
        for item in self.obj_lineage_tree.get_children():
            self.obj_lineage_tree.delete(item)
        for item in self.col_lineage_tree.get_children():
            self.col_lineage_tree.delete(item)

    def _update_object_lineage_display(self, mappings):
        """Update the object lineage treeview with new data"""
        for mapping in mappings:
            self.obj_lineage_tree.insert("", "end", values=mapping)

    def _update_column_lineage_display(self, mappings):
        """Update the column lineage treeview with new data"""
        for mapping in mappings:
            self.col_lineage_tree.insert("", "end", values=mapping)

    def _process_log_queue(self):
        """Process log messages from the queue"""
        while not self.log_queue.empty():
            message = self.log_queue.get()
            self.log_text.config(state="normal")
            self.log_text.insert("end", message + "\n")
            self.log_text.see("end")
            self.log_text.config(state="disabled")
        self.after(100, self._process_log_queue)

    def _fetch_and_update_models_threaded(self):
        """Fetch models in a background thread to avoid freezing the GUI."""
        thread = threading.Thread(target=self._fetch_and_update_models)
        thread.daemon = True
        thread.start()

    def _fetch_and_update_models(self):
        api_key = self.api_key_var.get()
        if not api_key:
            self._queue_gui_update(lambda: messagebox.showerror("Error", "Please enter your Gemini API Key first."))
            return

        logging.info("Fetching available models...")
        try:
            client = genai.Client(api_key=api_key)
            models = client.models.list()
            # Get all model names - the new API doesn't filter by supported_generation_methods
            model_names = [model.name for model in models if hasattr(model, 'name')]

            self._queue_gui_update(lambda: self._log_ai_interaction(f"Found {len(model_names)} available models"))

            if model_names:
                self._queue_gui_update(lambda: self.model_combo.config(values=model_names))
                logging.info(f"Successfully updated model list. Found {len(model_names)} models.")
                self._queue_gui_update(lambda: messagebox.showinfo("Success", f"Found {len(model_names)} available models."))
                # Update AI status to connected
                self._queue_gui_update(lambda: self._draw_status_circle(self.ai_status_canvas, "green"))
                self._queue_gui_update(lambda: self.ai_status_label.config(text="Connected"))
                self._queue_gui_update(lambda: self._log_ai_interaction("Model list updated successfully"))
            else:
                logging.warning("Could not find any compatible models.")
                self._queue_gui_update(lambda: messagebox.showwarning("Warning", "Could not find any compatible models for your API key."))
                # Update AI status to failed
                self._queue_gui_update(lambda: self._draw_status_circle(self.ai_status_canvas, "red"))
                self._queue_gui_update(lambda: self.ai_status_label.config(text="No Models"))
                self._queue_gui_update(lambda: self._log_ai_interaction("No compatible models found"))
        except Exception as e:
            error_msg = str(e)
            logging.error(f"Failed to fetch models: {error_msg}", exc_info=True)
            self._queue_gui_update(lambda: messagebox.showerror("Error", f"Failed to fetch models. Please check your API key and network connection.\n\n{error_msg}"))
            # Update AI status to failed
            self._queue_gui_update(lambda: self._draw_status_circle(self.ai_status_canvas, "red"))
            self._queue_gui_update(lambda: self.ai_status_label.config(text="Connection Failed"))
            self._queue_gui_update(lambda: self._log_ai_interaction(f"Failed to fetch models: {error_msg}"))

    def _generate_lineage_json(self, mappings, sql_content, output_dir):
        """Generate comprehensive JSON structure describing data flow relationships"""
        # Build nodes, connections, and transformations from mappings
        nodes = {}
        connections = []
        transformations = []

        for mapping in mappings:
            source_table = mapping.get("source_table") or mapping.get("source") or mapping.get("from_table")
            target_table = mapping.get("target_table") or mapping.get("target") or mapping.get("to_table")
            source_column = mapping.get("source_column") or mapping.get("from_column")
            target_column = mapping.get("target_column") or mapping.get("to_column")
            transformation_logic = mapping.get("logic") or mapping.get("transformation")

            # Add nodes
            if source_table and source_table not in nodes:
                nodes[source_table] = {"id": source_table, "name": source_table, "type": "table"}
            if target_table and target_table not in nodes:
                nodes[target_table] = {"id": target_table, "name": target_table, "type": "table"}

            # Add connection
            if source_table and target_table:
                connections.append({
                    "source": {"table": source_table, "column": source_column},
                    "target": {"table": target_table, "column": target_column},
                    "transformation": transformation_logic or ""
                })

        lineage_json = {
            "metadata": {
                "generated_at": "2025-01-09T00:00:00Z",
                "source_sql": sql_content[:100] + "..." if len(sql_content) > 100 else sql_content,
                "tool": "SQL Lineage Analyzer"
            },
            "nodes": list(nodes.values()),
            "connections": connections,
            "transformations": transformations
        }

        # Save JSON
        json_path = os.path.join(output_dir, "lineage_graph.json")
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(lineage_json, f, indent=2)
        logging.info(f"Lineage JSON saved to: {json_path}")

        return lineage_json

    def _generate_chart_from_json(self, lineage_json, output_format, output_dir):
        """Generate chart based on output format"""
        if output_format == "drawio":
            self._generate_drawio_chart(lineage_json, output_dir)
        elif output_format == "mermaid":
            self._generate_mermaid_chart(lineage_json, output_dir)
        elif output_format == "graphviz":
            self._generate_graphviz_chart(lineage_json, output_dir)

    def _generate_drawio_chart(self, lineage_json, output_dir):
        """Generate Draw.io XML from lineage JSON"""
        # Basic Draw.io XML structure
        drawio_xml = '''<?xml version="1.0" encoding="UTF-8"?>
<mxfile host="app.diagrams.net" modified="2025-08-28T08:00:00.000Z" agent="SQL Lineage Analyzer" version="1.0">
  <diagram name="Data Lineage" id="lineage">
    <mxGraphModel dx="1422" dy="794" grid="1" gridSize="10" guides="1" tooltips="1" connect="1" arrows="1" fold="1" page="1" pageScale="1" pageWidth="827" pageHeight="1169" math="0" shadow="0">
      <root>
        <mxCell id="0"/>
        <mxCell id="1" parent="0"/>
'''

        # Add nodes
        y_pos = 50
        for i, node in enumerate(lineage_json["nodes"]):
            x_pos = 50 + (i % 3) * 250
            if i > 0 and i % 3 == 0:
                y_pos += 150

            drawio_xml += f'''        <mxCell id="node_{i}" value="{node['name']}" style="rounded=1;whiteSpace=wrap;html=1;fillColor=#dae8fc;strokeColor=#6c8ebf;" vertex="1" parent="1">
          <mxGeometry x="{x_pos}" y="{y_pos}" width="200" height="80" as="geometry"/>
        </mxCell>
'''

        # Add connections
        for i, conn in enumerate(lineage_json["connections"]):
            source_idx = next((j for j, node in enumerate(lineage_json["nodes"]) if node["id"] == conn["source"]["table"]), 0)
            target_idx = next((j for j, node in enumerate(lineage_json["nodes"]) if node["id"] == conn["target"]["table"]), 0)

            drawio_xml += f'''        <mxCell id="edge_{i}" value="{conn.get('transformation', '')}" style="edgeStyle=orthogonalEdgeStyle;rounded=0;orthogonalLoop=1;jettySize=auto;html=1;" edge="1" parent="1" source="node_{source_idx}" target="node_{target_idx}">
          <mxGeometry relative="1" as="geometry"/>
        </mxCell>
'''

        drawio_xml += '''      </root>
    </mxGraphModel>
  </diagram>
</mxfile>'''

        # Define drawio_dir before use
        drawio_dir = output_dir
        drawio_path = os.path.join(drawio_dir, "data_lineage.drawio")
        with open(drawio_path, 'w', encoding='utf-8') as f:
            f.write(drawio_xml)
        logging.info(f"Draw.io chart saved to: {drawio_path}")

    def _generate_mermaid_chart(self, lineage_json, output_dir):
        """Generate Mermaid diagram from lineage JSON"""
        mermaid_content = "graph TD\n"

        # Add nodes
        for node in lineage_json["nodes"]:
            node_id = node["id"].replace(".", "_").replace("-", "_")
            mermaid_content += f"    {node_id}[\"{node['name']}\"]\n"

        # Add connections
        for conn in lineage_json["connections"]:
            source_id = conn["source"]["table"].replace(".", "_").replace("-", "_")
            target_id = conn["target"]["table"].replace(".", "_").replace("-", "_")
            label = conn.get("transformation", "")
            if label:
                mermaid_content += f"    {source_id} -->|{label}| {target_id}\n"
            else:
                mermaid_content += f"    {source_id} --> {target_id}\n"

        mermaid_path = os.path.join(output_dir, "data_lineage.mmd")
        with open(mermaid_path, 'w', encoding='utf-8') as f:
            f.write(mermaid_content)
        logging.info(f"Mermaid chart saved to: {mermaid_path}")

    def _generate_graphviz_chart(self, lineage_json, output_dir):
        """Generate Graphviz DOT file from lineage JSON"""
        dot_content = "digraph DataLineage {\n"
        dot_content += "    rankdir=LR;\n"
        dot_content += "    node [shape=box, style=filled, fillcolor=lightblue];\n"

        # Add nodes
        for node in lineage_json["nodes"]:
            node_id = node["id"].replace(".", "_").replace("-", "_")
            dot_content += f'    {node_id} [label="{node["name"]}"];\n'

        # Add connections
        for conn in lineage_json["connections"]:
            source_id = conn["source"]["table"].replace(".", "_").replace("-", "_")
            target_id = conn["target"]["table"].replace(".", "_").replace("-", "_")
            label = conn.get("transformation", "")
            if label:
                dot_content += f'    {source_id} -> {target_id} [label="{label}"];\n'
            else:
                dot_content += f'    {source_id} -> {target_id};\n'

        dot_content += "}\n"

        dot_path = os.path.join(output_dir, "data_lineage.dot")
        with open(dot_path, 'w', encoding='utf-8') as f:
            f.write(dot_content)
        logging.info(f"Graphviz DOT file saved to: {dot_path}")

    def _process_and_save_output(self, response_text, output_dir, sql_content=""):
        logging.info("Attempting to parse JSON response from AI.")
        try:
            match = re.search(r"```json\n(.*)\n```", response_text, re.DOTALL)
            cleaned_text = match.group(1) if match else response_text
            data = json.loads(cleaned_text)
            logging.info("Successfully parsed JSON.")
        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode JSON from AI response. Error: {e}")
            logging.debug(f"--- Full AI Response that failed parsing ---\n{response_text}\n--- End of AI Response ---")
            raise ValueError("Could not parse valid JSON from AI response.") from e

        mappings = data.get("mappings", [])
        diagram_xml = data.get("diagram", "")

        # Clean and validate mappings data
        mappings = self._clean_mappings_data(mappings)

        if mappings:
            object_lineage_path = os.path.join(output_dir, "object_lineage.csv")
            # Handle cases where mappings might have different key names or missing keys
            obj_mappings = []
            for m in mappings:
                try:
                    # Ensure m is a dictionary
                    if not isinstance(m, dict):
                        logging.warning(f"Skipping non-dictionary mapping entry: {m}")
                        continue

                    source = m.get('source_table', m.get('source', m.get('from_table', 'Unknown')))
                    target = m.get('target_table', m.get('target', m.get('to_table', 'Unknown')))

                    # Ensure we have valid string values
                    if source and target:
                        obj_mappings.append((str(source), str(target)))
                    else:
                        logging.warning(f"Skipping mapping with missing source/target: {m}")
                except Exception as e:
                    logging.warning(f"Skipping malformed mapping entry: {m}. Error: {e}")
                    continue

            # Remove duplicates - only convert to set if we have tuples
            if obj_mappings and all(isinstance(item, tuple) for item in obj_mappings):
                obj_mappings = list(set(obj_mappings))
            else:
                logging.warning("Some obj_mappings are not tuples, skipping deduplication")
                # Filter to only keep tuples
                obj_mappings = [item for item in obj_mappings if isinstance(item, tuple)]

            with open(object_lineage_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['source_table', 'target_table'])
                writer.writerows(obj_mappings)
            logging.info(f"Object lineage saved to: {object_lineage_path}")

            # Update GUI display for object lineage
            self._queue_gui_update(lambda: self._update_object_lineage_display(obj_mappings))

            column_lineage_path = os.path.join(output_dir, "column_lineage.csv")
            col_mappings = []
            with open(column_lineage_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['source_table', 'source_column', 'target_table', 'target_column'])
                for m in mappings:
                    try:
                        source_table = m.get('source_table', m.get('source', m.get('from_table', 'Unknown')))
                        source_column = m.get('source_column', m.get('source_col', m.get('from_column', 'Unknown')))
                        target_table = m.get('target_table', m.get('target', m.get('to_table', 'Unknown')))
                        target_column = m.get('target_column', m.get('target_col', m.get('to_column', 'Unknown')))
                        row_data = [source_table, source_column, target_table, target_column]
                        writer.writerow(row_data)
                        col_mappings.append(tuple(row_data))
                    except Exception as e:
                        logging.warning(f"Skipping malformed mapping entry for column lineage: {m}. Error: {e}")
                        continue
            logging.info(f"Column lineage saved to: {column_lineage_path}")

            # Update GUI display for column lineage
            self._queue_gui_update(lambda: self._update_column_lineage_display(col_mappings))

            # Generate comprehensive JSON and charts
            logging.info("Generating lineage JSON and charts...")
            # Filter mappings to only include dictionaries for JSON generation
            filtered_mappings = [m for m in mappings if isinstance(m, dict)]
            lineage_json = self._generate_lineage_json(filtered_mappings, sql_content, output_dir)
            output_format = self.output_format_var.get()
            self._generate_chart_from_json(lineage_json, output_format, output_dir)
        else:
            logging.warning("No mappings were generated by the AI.")

    def _clean_mappings_data(self, mappings):
        """Clean and validate mappings data, converting malformed entries when possible"""
        if not isinstance(mappings, list):
            logging.warning(f"Mappings is not a list: {type(mappings)}")
            return []

        cleaned_mappings = []

        for i, mapping in enumerate(mappings):
            try:
                if isinstance(mapping, dict):
                    # Already a proper dictionary, add it as-is
                    cleaned_mappings.append(mapping)
                elif isinstance(mapping, str):
                    # Try to convert string to a basic mapping
                    table_name = mapping.strip()
                    if table_name and '.' in table_name:
                        # Create a basic mapping for table references
                        cleaned_mapping = {
                            "source_table": table_name,
                            "target_table": "UNKNOWN_TARGET",
                            "source_column": "UNKNOWN_COLUMN",
                            "target_column": "UNKNOWN_COLUMN",
                            "transformation_type": "UNKNOWN"
                        }
                        cleaned_mappings.append(cleaned_mapping)
                        logging.info(f"Converted string '{table_name}' to basic mapping")
                    else:
                        logging.warning(f"Skipping malformed string mapping (no dots): {mapping}")
                else:
                    logging.warning(f"Skipping non-dict, non-string mapping entry: {type(mapping)} - {mapping}")

            except Exception as e:
                logging.warning(f"Error processing mapping entry {i}: {e}")
                continue

        logging.info(f"Cleaned mappings: {len(mappings)} original -> {len(cleaned_mappings)} cleaned")
        return cleaned_mappings

    def _on_logmech_change(self, event=None):
        """Handle logon mechanism change"""
        self._update_credential_fields()

    def _update_credential_fields(self):
        """Enable/disable username and password fields based on logon mechanism"""
        try:
            logmech = self.db_logmech_var.get()

            if logmech == "BROWSER":
                # Disable username and password for BROWSER authentication
                if hasattr(self, 'username_entry'):
                    self.username_entry.config(state="disabled")
                if hasattr(self, 'password_entry'):
                    self.password_entry.config(state="disabled")
            else:
                # Enable username and password for other authentication methods
                if hasattr(self, 'username_entry'):
                    self.username_entry.config(state="normal")
                if hasattr(self, 'password_entry'):
                    self.password_entry.config(state="normal")
        except Exception as e:
            print(f"Error updating credential fields: {e}")

    def _save_db_config(self):
        """Save database configuration to config file"""
        config = {
            "host": self.db_host_var.get(),
            "user": self.db_user_var.get(),
            "password": self.db_password_var.get(),
            "logmech": self.db_logmech_var.get(),
            "api_key": self.api_key_var.get()
        }
        save_config(config)

        # Update global db_connection_params
        global db_connection_params
        db_connection_params = config
        
        # Update teradata_tools with the new connection parameters
        update_db_connection_params(config)

        messagebox.showinfo("Saved", "Database and API configuration saved successfully!")

    def _log_interaction(self, message):
        """Log a message to the interactions text area"""
        timestamp = get_timestamp()
        formatted_message = f"[{timestamp}] {message}\n"

        self.interactions_text.config(state="normal")
        self.interactions_text.insert("end", formatted_message)
        self.interactions_text.see("end")
        self.interactions_text.config(state="disabled")

    def _clear_interactions_log(self):
        """Clear the interactions log"""
        self.interactions_text.config(state="normal")
        self.interactions_text.delete(1.0, "end")
        self.interactions_text.config(state="disabled")

    def _refresh_connectivity(self):
        """Force re-check of AI and DB connectivity from main thread."""
        thread_ai = threading.Thread(target=self._check_ai_connectivity)
        thread_ai.daemon = True
        thread_ai.start()
        thread_db = threading.Thread(target=self._check_database_connectivity)
        thread_db.daemon = True
        thread_db.start()

    def _check_initial_connectivity(self):
        """Check connectivity status on app startup"""
        print("DEBUG: Starting initial connectivity checks...")
        # Show user that we're checking connectivity
        self.title("SQL Lineage Analyzer - Checking Connectivity...")

        # Check AI connectivity in background
        thread = threading.Thread(target=self._check_ai_connectivity)
        thread.daemon = True
        thread.start()

        # Check database connectivity in background if configured
        if db_connection_params.get("host"):
            print("DEBUG: Starting database connectivity check...")
            thread = threading.Thread(target=self._check_database_connectivity)
            thread.daemon = True
            thread.start()
        else:
            print("DEBUG: No database host configured, skipping DB check")

        # Update title back after a short delay
        self.after(2000, lambda: self.title("SQL Lineage Analyzer"))

    def _draw_status_circle(self, canvas, color):
        """Draw a colored circle on the canvas"""
        print(f"DEBUG: Drawing status circle - canvas: {canvas}, color: {color}")
        canvas.delete("all")
        x, y, r = 10, 10, 8
        canvas.create_oval(x-r, y-r, x+r, y+r, fill=color, outline="black", width=1)

    def _update_status_indicators(self):
        """Update the traffic light status indicators"""
        # AI Status
        self._queue_gui_update(lambda: self._draw_status_circle(self.ai_status_canvas, "red"))
        self._queue_gui_update(lambda: self.ai_status_label.config(text="Not Connected"))

        # Database Status
        self._queue_gui_update(lambda: self._draw_status_circle(self.db_status_canvas, "red"))
        self._queue_gui_update(lambda: self.db_status_label.config(text="Not Connected"))

    def _log_interaction(self, message):
        """Log a message to the interactions text area - defaults to database"""
        self._log_db_interaction(message)

    def _clear_interactions_log(self):
        """Clear both interactions logs"""
        self._clear_db_interactions_log()
        self._clear_ai_interactions_log()

    def _log_db_interaction(self, message):
        """Log a message to the database interactions text area"""
        print(f"DEBUG: Logging DB interaction: {message}")
        timestamp = get_timestamp()
        formatted_message = f"[{timestamp}] {message}\n"

        self.db_interactions_text.config(state="normal")
        self.db_interactions_text.insert("end", formatted_message)
        self.db_interactions_text.see("end")
        self.db_interactions_text.config(state="disabled")

    def _log_ai_interaction(self, message):
        """Log a message to the AI interactions text area"""
        print(f"DEBUG: Logging AI interaction: {message}")
        timestamp = get_timestamp()
        formatted_message = f"[{timestamp}] {message}\n"

        self.ai_interactions_text.config(state="normal")
        self.ai_interactions_text.insert("end", formatted_message)
        self.ai_interactions_text.see("end")
        self.ai_interactions_text.config(state="disabled")

    def _clear_db_interactions_log(self):
        """Clear the database interactions log"""
        self.db_interactions_text.config(state="normal")
        self.db_interactions_text.delete(1.0, "end")
        self.db_interactions_text.config(state="disabled")

    def _clear_ai_interactions_log(self):
        """Clear the AI interactions log"""
        self.ai_interactions_text.config(state="normal")
        self.ai_interactions_text.delete(1.0, "end")
        self.ai_interactions_text.config(state="disabled")

    def _load_default_file(self):
        """Load test.sql as default file if it exists"""
        default_file = "test.sql"
        if os.path.exists(default_file):
            try:
                # Clear any existing files first
                self.file_listbox.delete(0, tk.END)
                # Add the default file
                self.file_listbox.insert(tk.END, default_file)
                logging.info(f"Loaded default file: {default_file}")
            except Exception as e:
                logging.warning(f"Could not load default file {default_file}: {e}")
        else:
            logging.info(f"Default file {default_file} not found, skipping auto-load")

    def _connect_to_teradata(self):
        """Handle the connect to Teradata button click"""
        # Clear previous interactions log
        self._clear_interactions_log()

        # Set database status to busy (orange)
        self._draw_status_circle(self.db_status_canvas, "orange")
        self.db_status_label.config(text="Connecting...")
        self.connect_button.config(state="disabled")

        # Start connectivity check in background thread
        thread = threading.Thread(target=self._check_database_connectivity)
        thread.daemon = True
        thread.start()

    def _check_database_connectivity(self):
        """Check database connectivity in background thread"""
        print("DEBUG: Starting database connectivity check...")
        try:
            print("DEBUG: Setting DB status to checking...")
            # Clear previous interactions for this connection attempt
            self._queue_gui_update(lambda: self._log_db_interaction("=== Starting Teradata Connection Test ==="))

            # Update global connection params
            global db_connection_params
            host = self.db_host_var.get().strip()
            user = self.db_user_var.get().strip()
            password = self.db_password_var.get().strip()
            logmech = self.db_logmech_var.get().strip()

            db_connection_params["host"] = host
            db_connection_params["user"] = user
            db_connection_params["password"] = password
            db_connection_params["logmech"] = logmech

            print(f"DEBUG: DB config - host: {host}, user: {user}, logmech: {logmech}")

            # Update teradata_tools with the connection parameters
            update_db_connection_params(db_connection_params)

            self._queue_gui_update(lambda: self._log_db_interaction(f"Host: {host}"))
            self._queue_gui_update(lambda: self._log_db_interaction(f"Logon Mechanism: {logmech}"))
            if logmech != "BROWSER":
                self._queue_gui_update(lambda: self._log_db_interaction(f"User: {user}"))
            else:
                self._queue_gui_update(lambda: self._log_db_interaction("User: Not required (BROWSER auth)"))

            # Construct and log the connection string
            connection_params = {
                "host": host,
                "user": user,
                "password": password,
                "logmech": logmech
            }
            connection_string = str(connection_params).replace("'", '"')
            self._queue_gui_update(lambda: self._log_db_interaction(f"Connection String: {connection_string}"))

            if not host:
                raise ValueError("Host is required for database connection")

            self._queue_gui_update(lambda: self._log_db_interaction("Attempting connection..."))
            print("DEBUG: Attempting database connection...")

            # Try to connect using the same method as teradata_tools
            with teradatasql.connect(connection_string) as conn:
                self._queue_gui_update(lambda: self._log_db_interaction("Connection established successfully"))
                print("DEBUG: Database connection established successfully")

                # Execute a simple query to test connection
                cursor = conn.cursor()
                self._queue_gui_update(lambda: self._log_db_interaction("Executing test query: SELECT 1 as test"))

                cursor.execute("SELECT 1 as test")
                result = cursor.fetchone()
                cursor.close()

                if result:
                    print("DEBUG: Database connection test successful")
                    # Success - set to green
                    self._queue_gui_update(lambda: self._draw_status_circle(self.db_status_canvas, "green"))
                    self._queue_gui_update(lambda: self.db_status_label.config(text="Connected"))
                    self._queue_gui_update(lambda: self._log_db_interaction(f"Test query successful: {result}"))
                    self._queue_gui_update(lambda: self._log_db_interaction("=== Connection Test PASSED ==="))
                    self._queue_gui_update(lambda: messagebox.showinfo("Database Connection", "Database connection successful! Test query executed."))
                    logging.info("Database connection successful")
                else:
                    raise ValueError("Connection test query returned no results")

        except teradatasql.DatabaseError as db_error:
            # Teradata-specific database error
            error_code = getattr(db_error, 'code', 'Unknown')
            error_msg = str(db_error)
            print(f"DEBUG: Database error: {error_msg}")
            self._queue_gui_update(lambda: self._log_db_interaction(f"Database Error [{error_code}]: {error_msg}"))
            self._queue_gui_update(lambda: self._log_db_interaction("=== Connection Test FAILED ==="))

            # Failed - set to red
            self._queue_gui_update(lambda: self._draw_status_circle(self.db_status_canvas, "red"))
            self._queue_gui_update(lambda: self.db_status_label.config(text="Connection Failed"))
            self._queue_gui_update(lambda: messagebox.showerror("Database Connection Failed", f"Database error: {error_msg}"))
            logging.error(f"Database connection failed: {error_msg}")

        except teradatasql.InterfaceError as iface_error:
            # Connection/interface error
            error_code = getattr(iface_error, 'code', 'Unknown')
            error_msg = str(iface_error)
            print(f"DEBUG: Interface error: {error_msg}")
            self._queue_gui_update(lambda: self._log_db_interaction(f"Interface Error [{error_code}]: {error_msg}"))
            self._queue_gui_update(lambda: self._log_db_interaction("=== Connection Test FAILED ==="))

            # Failed - set to red
            self._queue_gui_update(lambda: self._draw_status_circle(self.db_status_canvas, "red"))
            self._queue_gui_update(lambda: self.db_status_label.config(text="Connection Failed"))
            self._queue_gui_update(lambda: messagebox.showerror("Database Connection Failed", f"Interface error: {error_msg}"))
            logging.error(f"Database interface error: {error_msg}")

        except Exception as e:
            # Generic error
            error_msg = str(e)
            print(f"DEBUG: Generic database error: {error_msg}")
            self._queue_gui_update(lambda: self._log_db_interaction(f"Error: {error_msg}"))
            self._queue_gui_update(lambda: self._log_db_interaction("=== Connection Test FAILED ==="))

            # Failed - set to red
            self._queue_gui_update(lambda: self._draw_status_circle(self.db_status_canvas, "red"))
            self._queue_gui_update(lambda: self.db_status_label.config(text="Connection Failed"))
            self._queue_gui_update(lambda: messagebox.showerror("Database Connection Failed", f"Connection error: {error_msg}"))
            logging.error(f"Database connection failed: {error_msg}")

        finally:
            # Re-enable connect button
            self._queue_gui_update(lambda: self.connect_button.config(state="normal"))

    def _check_ai_connectivity(self):
        """Check AI connectivity"""
        print("DEBUG: Starting AI connectivity check...")
        api_key = self.api_key_var.get().strip()
        if not api_key:
            print("DEBUG: No API key found")
            self._queue_gui_update(lambda: self._draw_status_circle(self.ai_status_canvas, "red"))
            self._queue_gui_update(lambda: self.ai_status_label.config(text="No API Key"))
            self._queue_gui_update(lambda: messagebox.showwarning("AI Configuration", "No API key configured. Please enter your Gemini API key."))
            return

        try:
            print("DEBUG: Setting AI status to checking...")
            # Set to busy
            self._queue_gui_update(lambda: self._draw_status_circle(self.ai_status_canvas, "orange"))
            self._queue_gui_update(lambda: self.ai_status_label.config(text="Checking..."))

            # Try to create client and list models
            self._queue_gui_update(lambda: self._log_ai_interaction("=== Checking AI Connectivity ==="))
            self._queue_gui_update(lambda: self._log_ai_interaction("Creating Gemini client..."))

            client = genai.Client(api_key=api_key)
            self._queue_gui_update(lambda: self._log_ai_interaction("Fetching available models..."))

            models = client.models.list()

            if models:
                model_count = len(list(models))
                print(f"DEBUG: AI connection successful, found {model_count} models")
                # Success - set to green
                self._queue_gui_update(lambda: self._draw_status_circle(self.ai_status_canvas, "green"))
                self._queue_gui_update(lambda: self.ai_status_label.config(text="Connected"))
                self._queue_gui_update(lambda: self._log_ai_interaction(f"Successfully retrieved {model_count} models"))
                self._queue_gui_update(lambda: self._log_ai_interaction("=== AI Connection Test PASSED ==="))
                self._queue_gui_update(lambda: messagebox.showinfo("AI Connection", f"AI connection successful! Found {model_count} models."))
                logging.info("AI connection successful")
            else:
                raise ValueError("No models available")

        except Exception as e:
            print(f"DEBUG: AI connection failed: {e}")
            # Failed - set to red
            error_msg = str(e)
            self._queue_gui_update(lambda: self._draw_status_circle(self.ai_status_canvas, "red"))
            self._queue_gui_update(lambda: self.ai_status_label.config(text="Connection Failed"))
            self._queue_gui_update(lambda: self._log_ai_interaction(f"AI Error: {error_msg}"))
            self._queue_gui_update(lambda: self._log_ai_interaction("=== AI Connection Test FAILED ==="))
            self._queue_gui_update(lambda: messagebox.showerror("AI Connection Failed", f"AI connection failed: {error_msg}"))
            logging.error(f"AI connection failed: {error_msg}")

# --- Main application entry point ---
if __name__ == "__main__":
    app = SQLAnalyzerApp()
    app.mainloop()