import tkinter as tk
from tkinter import ttk, filedialog, messagebox
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

# --- Local Imports ---
from teradata_tools import get_object_ddl, db_connection_params

# --- GUI Classes ---
class QueueHandler(logging.Handler):
    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record):
        self.log_queue.put(record)

class AnalysisStatusWindow(tk.Toplevel):
    def __init__(self, parent, sql_content):
        super().__init__(parent)
        self.title("Analysis in Progress...")
        self.geometry("600x400")
        self.transient(parent)
        self.grab_set()

        self.status_var = tk.StringVar(value="Initializing...")
        status_label = ttk.Label(self, textvariable=self.status_var, font=("Helvetica", 10, "bold"))
        status_label.pack(padx=10, pady=10)

        text_frame = ttk.Frame(self)
        text_frame.pack(padx=10, pady=5, fill="both", expand=True)

        sql_text = tk.Text(text_frame, wrap="word", height=10)
        sql_text.insert(tk.END, sql_content)
        sql_text.config(state="disabled")
        sql_text.pack(side="left", fill="both", expand=True)

        scrollbar = ttk.Scrollbar(text_frame, orient="vertical", command=sql_text.yview)
        scrollbar.pack(side="right", fill="y")
        sql_text.config(yscrollcommand=scrollbar.set)

        self.update_idletasks()

    def update_status(self, message):
        self.status_var.set(message)
        self.update_idletasks()

    def close_window(self):
        self.grab_release()
        self.destroy()

class SQLAnalyzerApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("SQL Lineage Analyzer")
        self.geometry("800x700")

        # --- Setup ---
        self._setup_logging()
        self._load_prompts()
        self._create_widgets()

    def _setup_logging(self):
        self.log_queue = queue.Queue()
        self.queue_handler = QueueHandler(self.log_queue)
        formatter = logging.Formatter('%(asctime)s: %(levelname)s: %(message)s')
        self.queue_handler.setFormatter(formatter)
        
        logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(levelname)s: %(message)s')
        self.logger = logging.getLogger()
        self.logger.addHandler(self.queue_handler)

        file_handler = logging.FileHandler("analysis.log")
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def _load_prompts(self):
        try:
            with open("prompts.yaml", 'r') as f:
                self.prompts = yaml.safe_load(f)
            logging.info("Successfully loaded prompts from prompts.yaml")
        except FileNotFoundError:
            logging.error("CRITICAL: prompts.yaml not found.")
            messagebox.showerror("Critical Error", "prompts.yaml not found! Please ensure it exists.")
            self.destroy()
        except yaml.YAMLError as e:
            logging.error(f"CRITICAL: Error parsing prompts.yaml: {e}")
            messagebox.showerror("Critical Error", f"Error parsing prompts.yaml: {e}")
            self.destroy()

    def _create_widgets(self):
        self.api_key_var = tk.StringVar(value=os.environ.get("GEMINI_API_KEY", ""))
        self.model_var = tk.StringVar(value="gemini-1.5-flash-latest")
        self.db_enabled_var = tk.BooleanVar(value=False)
        self.db_host_var = tk.StringVar(value="TeraDW")
        self.sql_files_var = tk.StringVar(value="No files selected.")
        self.output_dir_var = tk.StringVar(value=os.getcwd())
        self.output_format_var = tk.StringVar(value="drawio")
        self.selected_files = []
        
        # Auto-select test.sql if it exists
        test_sql_path = os.path.join(os.getcwd(), "test.sql")
        if os.path.exists(test_sql_path):
            self.selected_files = [test_sql_path]
            self.sql_files_var.set("1 file(s) selected.")
            logging.info(f"Auto-selected default file: test.sql")

        config_frame = ttk.LabelFrame(self, text="Configuration", padding="10")
        config_frame.pack(side="top", fill="x", padx=10, pady=5)
        ttk.Label(config_frame, text="Gemini API Key:").grid(row=0, column=0, padx=5, pady=5, sticky="w")
        ttk.Entry(config_frame, textvariable=self.api_key_var, width=40, show="*").grid(row=0, column=1, padx=5, pady=5, sticky="ew")
        ttk.Label(config_frame, text="Gemini Model:").grid(row=0, column=2, padx=5, pady=5, sticky="w")
        self.model_combo = ttk.Combobox(config_frame, textvariable=self.model_var, values=["gemini-1.5-flash-latest", "gemini-1.5-pro-latest"])
        self.model_combo.grid(row=0, column=3, padx=5, pady=5, sticky="ew")
        ttk.Button(config_frame, text="Refresh", command=self._fetch_and_update_models, width=10).grid(row=0, column=4, padx=5, pady=5)
        config_frame.columnconfigure(1, weight=1)
        config_frame.columnconfigure(3, weight=1)

        db_frame = ttk.LabelFrame(self, text="Teradata Connection (Optional)", padding="10")
        db_frame.pack(side="top", fill="x", padx=10, pady=5)
        ttk.Checkbutton(db_frame, text="Use DB Metadata to Improve Analysis (via Browser Auth)", variable=self.db_enabled_var).grid(row=0, column=0, columnspan=2, padx=5, pady=5, sticky="w")
        ttk.Label(db_frame, text="Host:").grid(row=1, column=0, padx=5, pady=5, sticky="w")
        ttk.Entry(db_frame, textvariable=self.db_host_var).grid(row=1, column=1, padx=5, pady=5, sticky="ew")
        db_frame.columnconfigure(1, weight=1)

        file_frame = ttk.LabelFrame(self, text="Inputs & Outputs", padding="10")
        file_frame.pack(side="top", fill="x", padx=10, pady=5)
        ttk.Label(file_frame, text="SQL Files:").grid(row=0, column=0, padx=5, pady=5, sticky="w")
        ttk.Label(file_frame, textvariable=self.sql_files_var).grid(row=0, column=1, padx=5, pady=5, sticky="ew")
        self.browse_files_button = ttk.Button(file_frame, text="Select Files...", command=self.select_sql_files)
        self.browse_files_button.grid(row=0, column=2, padx=5, pady=5)
        ttk.Label(file_frame, text="Output Directory:").grid(row=1, column=0, padx=5, pady=5, sticky="w")
        ttk.Label(file_frame, textvariable=self.output_dir_var).grid(row=1, column=1, padx=5, pady=5, sticky="ew")
        self.browse_dir_button = ttk.Button(file_frame, text="Select Directory...", command=self.select_output_dir)
        self.browse_dir_button.grid(row=1, column=2, padx=5, pady=5)
        ttk.Label(file_frame, text="Chart Format:").grid(row=2, column=0, padx=5, pady=5, sticky="w")
        self.format_combo = ttk.Combobox(file_frame, textvariable=self.output_format_var, values=["drawio", "mermaid", "graphviz"], state="readonly")
        self.format_combo.grid(row=2, column=1, padx=5, pady=5, sticky="ew")
        file_frame.columnconfigure(1, weight=1)

        action_frame = ttk.Frame(self, padding="10")
        action_frame.pack(side="top", fill="x", padx=10, pady=10)
        self.run_button = ttk.Button(action_frame, text="Run Analysis", command=self.run_analysis_threaded)
        self.run_button.pack(pady=5)

        # Create a notebook for tabbed display of results
        results_notebook = ttk.Notebook(self)
        results_notebook.pack(side="top", fill="both", expand=True, padx=10, pady=5)

        # Object Lineage Tab
        obj_lineage_frame = ttk.Frame(results_notebook)
        results_notebook.add(obj_lineage_frame, text="Object Lineage")
        
        obj_lineage_label = ttk.Label(obj_lineage_frame, text="Object-to-Object Lineage", font=("Helvetica", 10, "bold"))
        obj_lineage_label.pack(padx=5, pady=5)
        
        obj_tree_frame = ttk.Frame(obj_lineage_frame)
        obj_tree_frame.pack(fill="both", expand=True, padx=5, pady=5)
        
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
        col_lineage_frame = ttk.Frame(results_notebook)
        results_notebook.add(col_lineage_frame, text="Column Lineage")
        
        col_lineage_label = ttk.Label(col_lineage_frame, text="Column-to-Column Lineage", font=("Helvetica", 10, "bold"))
        col_lineage_label.pack(padx=5, pady=5)
        
        col_tree_frame = ttk.Frame(col_lineage_frame)
        col_tree_frame.pack(fill="both", expand=True, padx=5, pady=5)
        
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
        log_frame = ttk.Frame(results_notebook)
        results_notebook.add(log_frame, text="Logs")
        
        self.log_text = tk.Text(log_frame, height=10, state="disabled")
        self.log_text.pack(side="left", fill="both", expand=True)
        log_scrollbar = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview)
        log_scrollbar.pack(side="right", fill="y")
        self.log_text.config(yscrollcommand=log_scrollbar.set)
        self.after(100, self._process_log_queue)

    def _clear_lineage_displays(self):
        """Clear the lineage display tables"""
        for item in self.obj_lineage_tree.get_children():
            self.obj_lineage_tree.delete(item)
        for item in self.col_lineage_tree.get_children():
            self.col_lineage_tree.delete(item)

    def _update_object_lineage_display(self, obj_mappings):
        """Update the object lineage display table"""
        # Clear existing data
        for item in self.obj_lineage_tree.get_children():
            self.obj_lineage_tree.delete(item)
        
        # Add new data
        for source, target in obj_mappings:
            self.obj_lineage_tree.insert("", "end", values=(source, target))

    def _update_column_lineage_display(self, col_mappings):
        """Update the column lineage display table"""
        # Clear existing data
        for item in self.col_lineage_tree.get_children():
            self.col_lineage_tree.delete(item)
        
        # Add new data
        for source_table, source_column, target_table, target_column in col_mappings:
            self.col_lineage_tree.insert("", "end", values=(source_table, source_column, target_table, target_column))

    def _process_log_queue(self):
        while not self.log_queue.empty():
            record = self.log_queue.get()
            msg = self.queue_handler.format(record)
            self.log_text.config(state="normal")
            self.log_text.insert(tk.END, msg + "\n")
            self.log_text.config(state="disabled")
            self.log_text.see(tk.END)
        self.after(100, self._process_log_queue)

    def select_sql_files(self):
        self.selected_files = filedialog.askopenfilenames(title="Select SQL Files", filetypes=(("SQL Files", "*.sql"), ("All files", "*.* sviluppo")))
        if self.selected_files:
            self.sql_files_var.set(f"{len(self.selected_files)} file(s) selected.")
            logging.info(f"Selected files: {', '.join(os.path.basename(f) for f in self.selected_files)}")

    def select_output_dir(self):
        directory = filedialog.askdirectory(title="Select Output Directory")
        if directory:
            self.output_dir_var.set(directory)
            logging.info(f"Output directory set to: {directory}")

    def run_analysis_threaded(self):
        logging.info("GUI thread: Kicking off analysis.")
        if not self.api_key_var.get() or not self.selected_files:
            messagebox.showerror("Error", "API Key and at least one SQL file are required.")
            return

        sql_content = ""
        for filepath in self.selected_files:
            with open(filepath, 'r', encoding='utf-8') as f:
                sql_content += f"-- Content from {os.path.basename(filepath)}\n{f.read()}\n\n"

        status_window = AnalysisStatusWindow(self, sql_content)
        thread = threading.Thread(target=self.run_analysis, args=(sql_content, status_window))
        thread.daemon = True
        thread.start()

    def run_analysis(self, sql_content, status_window):
        try:
            status_window.update_status("Starting analysis...")
            api_key = self.api_key_var.get()
            model_name = self.model_var.get()
            output_dir = self.output_dir_var.get()
            use_db = self.db_enabled_var.get()

            self.run_button.config(state="disabled")
            self.browse_files_button.config(state="disabled")
            
            # Clear previous lineage displays
            self.after(0, self._clear_lineage_displays)

            status_window.update_status("Formatting SQL with sqlfluff...")
            logging.info("Formatting SQL with sqlfluff...")
            try:
                formatted_sql = sqlfluff.fix(sql_content, dialect="teradata")
                logging.info("SQL formatting successful.")
            except Exception as e:
                logging.warning(f"SQLFluff formatting failed: {e}. Proceeding with raw SQL.")
                formatted_sql = sql_content

            # Configure the client with API key
            client = genai.Client(api_key=api_key)
            
            tools_to_use = [get_object_ddl] if use_db else None

            if use_db:
                db_connection_params["host"] = self.db_host_var.get()
                logging.info(f"Database tools enabled for host: {db_connection_params['host']}")
            
            prompt = self.prompts['main_analysis'].format(sql_content=formatted_sql)
            
            status_window.update_status("Sending prompt to Gemini. AI will now call functions as needed...")
            # Generate content using the new google.genai API
            if use_db and tools_to_use:
                response = client.models.generate_content(
                    model=model_name,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        tools=tools_to_use
                    )
                )
            else:
                response = client.models.generate_content(
                    model=model_name,
                    contents=prompt
                )
            
            status_window.update_status("AI analysis complete. Processing final response...")
            final_response_text = response.text
            self._process_and_save_output(final_response_text, output_dir, sql_content)
            messagebox.showinfo("Success", "Analysis complete! Output files have been saved.")

        except Exception as e:
            logging.error(f"An error occurred: {e}", exc_info=True)
            messagebox.showerror("Error", f"An error occurred during analysis:\n{e}")
        finally:
            self.run_button.config(state="normal")
            self.browse_files_button.config(state="normal")
            status_window.close_window()
            logging.info("Analysis finished.")

    def _generate_lineage_json(self, mappings, sql_content, output_dir):
        """Generate comprehensive JSON structure describing data flow relationships"""
        # Extract unique nodes
        nodes = {}
        connections = []
        transformations = []
        
        for m in mappings:
            if not isinstance(m, dict):
                continue
                
            source_table = m.get('source_table', m.get('source', m.get('from_table', 'Unknown')))
            source_column = m.get('source_column', m.get('source_col', m.get('from_column', '')))
            target_table = m.get('target_table', m.get('target', m.get('to_table', 'Unknown')))
            target_column = m.get('target_column', m.get('target_col', m.get('to_column', '')))
            transformation_logic = m.get('transformation', m.get('logic', m.get('description', '')))
            
            # Add source node
            if source_table not in nodes:
                nodes[source_table] = {
                    "id": source_table,
                    "name": source_table,
                    "type": "table",
                    "columns": []
                }
            
            if source_column and source_column not in [col["name"] for col in nodes[source_table]["columns"]]:
                nodes[source_table]["columns"].append({
                    "name": source_column,
                    "type": "column"
                })
            
            # Add target node
            if target_table not in nodes:
                nodes[target_table] = {
                    "id": target_table,
                    "name": target_table,
                    "type": "table",
                    "columns": []
                }
            
            if target_column and target_column not in [col["name"] for col in nodes[target_table]["columns"]]:
                nodes[target_table]["columns"].append({
                    "name": target_column,
                    "type": "column"
                })
            
            # Add connection
            connection = {
                "source": {
                    "table": source_table,
                    "column": source_column
                },
                "target": {
                    "table": target_table,
                    "column": target_column
                },
                "transformation": transformation_logic
            }
            connections.append(connection)
            
            # Add transformation if present
            if transformation_logic:
                transformations.append({
                    "source": f"{source_table}.{source_column}" if source_column else source_table,
                    "target": f"{target_table}.{target_column}" if target_column else target_table,
                    "logic": transformation_logic
                })
        
        # Create comprehensive JSON structure
        lineage_json = {
            "metadata": {
                "generated_at": "2025-08-28T" + "08:00:00Z",
                "source_sql": sql_content[:500] + "..." if len(sql_content) > 500 else sql_content,
                "tool": "SQL Lineage Analyzer",
                "version": "1.0"
            },
            "nodes": list(nodes.values()),
            "connections": connections,
            "transformations": transformations,
            "graph": {
                "directed": True,
                "type": "data_lineage"
            }
        }
        
        # Save JSON file
        json_path = os.path.join(output_dir, "lineage_graph.json")
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(lineage_json, f, indent=2, ensure_ascii=False)
        logging.info(f"Lineage JSON saved to: {json_path}")
        
        return lineage_json

    def _generate_chart_from_json(self, lineage_json, output_format, output_dir):
        """Generate chart files based on the lineage JSON and specified format"""
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

if __name__ == "__main__":
    app = SQLAnalyzerApp()
    app.mainloop()