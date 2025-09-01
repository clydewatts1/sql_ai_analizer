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
# Import get_object_ddl for Gemini function calling
from teradata_tools import get_object_ddl

# --- Local Imports ---

# --- Config file logic ---
CONFIG_DIR = os.path.join(os.path.expanduser("~"), "sql_an_analizer")
CONFIG_PATH = os.path.join(CONFIG_DIR, "config.yaml")

def load_config():
    if not os.path.exists(CONFIG_PATH):
        return {"host": "", "user": "", "password": "", "logmech": "BROWSER"}
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f) or {"host": "", "user": "", "password": "", "logmech": "BROWSER"}

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

# --- GUI Classes ---
class QueueHandler(logging.Handler):
    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record):
        self.log_queue.put(self.format(record))

class SQLAnalyzerApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("SQL Lineage Analyzer")
        self.geometry("1200x800")

        # Initialize variables
        self.api_key_var = tk.StringVar()
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

        # Create GUI
        self._create_widgets()

        # Start log processing
        self.after(100, self._process_log_queue)

    def _load_prompts(self):
        try:
            with open("prompts.yaml", "r") as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            messagebox.showerror("Error", "prompts.yaml file not found!")
            return {"main_analysis": "Analyze this SQL: {sql_content}"}

    def _create_widgets(self):
        # Main frame
        main_frame = ttk.Frame(self)
        main_frame.pack(fill="both", expand=True, padx=10, pady=10)

        # Configuration frame
        config_frame = ttk.LabelFrame(main_frame, text="Configuration", padding="10")
        config_frame.pack(fill="x", pady=(0, 10))

        # API Key
        ttk.Label(config_frame, text="Gemini API Key:").grid(row=0, column=0, sticky="w", padx=(0, 10))
        ttk.Entry(config_frame, textvariable=self.api_key_var, width=50, show="*").grid(row=0, column=1, sticky="ew", padx=(0, 10))
        ttk.Button(config_frame, text="Fetch Models", command=self._fetch_and_update_models).grid(row=0, column=2)

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
                                    values=["BROWSER", "LDAP", "TDNEGO", "KERBEROS", "SPNEGO"])
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
        ttk.Button(db_config_frame, text="Save DB Config", command=self._save_db_config).grid(row=4, column=1, sticky="e", pady=(10, 0))

        # Configure grid weights
        db_config_frame.columnconfigure(1, weight=1)

        # Initialize username/password field states
        self._update_credential_fields()

        # Action frame
        action_frame = ttk.Frame(main_frame)
        action_frame.pack(fill="x", pady=(0, 10))

        # File selection
        ttk.Label(action_frame, text="SQL Files:").pack(side="left", padx=(0, 10))
        self.file_listbox = tk.Listbox(action_frame, height=1, width=50)
        self.file_listbox.pack(side="left", fill="x", expand=True, padx=(0, 10))
        ttk.Button(action_frame, text="Browse Files", command=self._browse_files).pack(side="left", padx=(0, 10))
        ttk.Button(action_frame, text="Clear Files", command=self._clear_files).pack(side="left", padx=(0, 10))

        # Run button
        self.run_button = ttk.Button(action_frame, text="Run Analysis", command=self.run_analysis_threaded)
        self.run_button.pack(side="right")

        # Results notebook
        self.results_notebook = ttk.Notebook(main_frame)
        self.results_notebook.pack(fill="both", expand=True)

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
            messagebox.showwarning("No Files", "Please select at least one SQL file to analyze.")
            return

        # Check API key
        api_key = self.api_key_var.get()
        if not api_key:
            messagebox.showerror("No API Key", "Please enter your Gemini API Key.")
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
                tools_to_use = [get_object_ddl] if use_db else None

                if use_db:
                    db_connection_params["host"] = self.db_host_var.get()
                    db_connection_params["user"] = self.db_user_var.get()
                    db_connection_params["password"] = self.db_password_var.get()
                    db_connection_params["logmech"] = self.db_logmech_var.get()
                    logging.info(f"Database tools enabled for host: {db_connection_params['host']}")

                prompt = self.prompts['main_analysis'].format(sql_content=formatted_sql)

                # Generate content using the new google.genai API
                if use_db and tools_to_use:
                    response = client.models.generate_content(
                        model=self.model_var.get(),
                        contents=prompt,
                        config=types.GenerateContentConfig(
                            tools=tools_to_use
                        )
                    )
                else:
                    response = client.models.generate_content(
                        model=self.model_var.get(),
                        contents=prompt
                    )

                final_response_text = response.text
                output_dir = self.output_dir_var.get()
                self._process_and_save_output(final_response_text, output_dir, sql_content)

            messagebox.showinfo("Success", "Analysis complete! Output files have been saved.")

        except Exception as e:
            logging.error(f"An error occurred: {e}", exc_info=True)
            messagebox.showerror("Error", f"An error occurred during analysis:\n{e}")

        finally:
            # Re-enable run button
            self.run_button.config(state="normal")

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

    def _fetch_and_update_models(self):
        api_key = self.api_key_var.get()
        if not api_key:
            messagebox.showerror("Error", "Please enter your Gemini API Key first.")
            return

        logging.info("Fetching available models...")
        try:
            client = genai.Client(api_key=api_key)
            models = client.models.list()
            # Get all model names - the new API doesn't filter by supported_generation_methods
            model_names = [model.name for model in models if hasattr(model, 'name')]
            if model_names:
                self.model_combo['values'] = model_names
                logging.info(f"Successfully updated model list. Found {len(model_names)} models.")
                messagebox.showinfo("Success", f"Found {len(model_names)} available models.")
            else:
                logging.warning("Could not find any compatible models.")
                messagebox.showwarning("Warning", "Could not find any compatible models for your API key.")
        except Exception as e:
            logging.error(f"Failed to fetch models: {e}", exc_info=True)
            messagebox.showerror("Error", f"Failed to fetch models. Please check your API key and network connection.\n\n{e}")

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

        drawio_path = os.path.join(output_dir, "data_lineage.drawio")
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
            self.after(0, lambda: self._update_object_lineage_display(obj_mappings))

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
            self.after(0, lambda: self._update_column_lineage_display(col_mappings))

            # Generate comprehensive JSON and charts
            logging.info("Generating lineage JSON and charts...")
            lineage_json = self._generate_lineage_json(mappings, sql_content, output_dir)
            output_format = self.output_format_var.get()
            self._generate_chart_from_json(lineage_json, output_format, output_dir)
        else:
            logging.warning("No mappings were generated by the AI.")

        if diagram_xml:
            diagram_path = os.path.join(output_dir, "data_flow.drawio")
            with open(diagram_path, 'w', encoding='utf-8') as f:
                # Handle case where diagram_xml might be a list
                if isinstance(diagram_xml, list):
                    diagram_content = '\n'.join(str(item) for item in diagram_xml)
                else:
                    diagram_content = str(diagram_xml)
                f.write(diagram_content)
            logging.info(f"Draw.io diagram saved to: {diagram_path}")
        else:
            logging.warning("No diagram was generated by the AI.")

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
            "logmech": self.db_logmech_var.get()
        }
        save_config(config)

        # Update global db_connection_params
        global db_connection_params
        db_connection_params = config

        messagebox.showinfo("Saved", "Database configuration saved successfully!")

if __name__ == "__main__":
    app = SQLAnalyzerApp()
    app.mainloop()