# SQL Lineage Analyzer - AI Agent Instructions

## Architecture Overview
This is a **Tkinter-based GUI application** that analyzes SQL files using Google's Gemini AI to generate data lineage mappings. The app has two main components:

- **`sql_analyzer.py`**: Main GUI application with threading for AI analysis
      make sure the import is from google import genai
- **`teradata_tools.py`**: Database connectivity module for schema discovery via browser authentication

## Key Workflows

### Development Setup
```bash
# Always activate virtual environment first
.\venv\Scripts\Activate.ps1
python sql_analyzer.py
```

### AI Integration Pattern
The app uses **function calling** with Gemini AI where the AI can call `get_object_ddl()` to retrieve table schemas during analysis. The workflow:
1. User provides SQL → 2. AI analyzes and calls DDL functions → 3. AI returns JSON with mappings

### Critical Files Structure
- **`prompts.yaml`**: Contains the main AI prompt template (required at startup)
- **`requirements.txt`**: Uses `google-generativeai>=2.0.0` (not google-genai)
- **`test.sql`**: Auto-selected default input file
- **Output files**: `object_lineage.csv`, `column_lineage.csv`, `lineage_graph.json`, chart files (`.drawio`, `.mmd`, `.dot`)

## Import Dependencies Fix
**Critical**: The imports are correct. Use:
```python
from google import genai  # This is the correct import for latest google-genai library
from google.genai import types  # For type definitions
```

## API Usage Pattern
The app uses the new google-genai client-based API:
```python
client = genai.Client(api_key=api_key)
response = client.models.generate_content(
    model=model_name,
    contents=prompt,
    config=types.GenerateContentConfig(tools=tools) if tools else None
)
```

## API Quota Management
The app uses Google's Gemini API with free tier limits. Common errors:
- **429 RESOURCE_EXHAUSTED**: Hit quota limits (requests/minute, tokens/minute, requests/day)
- **Solution**: Wait for retry delay (shown in error) or upgrade API plan
- **Default models**: `gemini-1.5-flash-latest` (lower quota usage) vs `gemini-1.5-pro-latest` (higher accuracy)

## JSON Response Handling Pattern
The app expects AI responses in this exact structure:
```json
{
  "mappings": [
    {
      "source_table": "DB.TABLE1", 
      "target_table": "DB.TABLE2",
      "source_column": "COL1",
      "target_column": "COL2"
    }
  ],
  "diagram": "drawio_xml_content"
}
```

**Flexible key mapping**: The parser checks multiple key variations (`source_table`/`source`/`from_table`) to handle AI response variations.

## Threading Architecture
- **GUI thread**: Handles UI interactions
- **Analysis thread**: Runs AI processing to prevent UI freezing
- **Status window**: Modal dialog showing progress during analysis
- **Log queue**: Thread-safe logging between analysis and GUI threads

## Error Handling Patterns
- **JSON parsing**: Robust error handling with detailed logging and fallback strategies
- **Type checking**: Validates dictionary structures before processing mappings
- **Tuple deduplication**: Filters non-tuple entries before set conversion

## Database Integration
- **Teradata connectivity**: Uses browser-based authentication (`logmech: "BROWSER"`)
- **Function calling**: AI can call `get_object_ddl(table_name)` for schema discovery
- **Connection params**: Global `db_connection_params` dictionary shared between modules

## Chart Generation System
The app generates multiple output formats from a comprehensive JSON structure:

### JSON Structure (`lineage_graph.json`)
```json
{
  "metadata": { "generated_at": "...", "source_sql": "...", "tool": "..." },
  "nodes": [{ "id": "TABLE1", "name": "DB.TABLE1", "type": "table", "columns": [...] }],
  "connections": [{ "source": {...}, "target": {...}, "transformation": "..." }],
  "transformations": [{ "source": "...", "target": "...", "logic": "..." }]
}
```

### Chart Formats
- **Draw.io** (`.drawio`): XML format for diagrams.net
- **Mermaid** (`.mmd`): Text-based diagram format
- **Graphviz** (`.dot`): DOT language for graph visualization

**Format selection**: User chooses via dropdown in GUI, handled by `_generate_chart_from_json()` method.

## GUI Components
- **Configuration panel**: API key, model selection, database options
- **File selection**: Multi-file SQL input with auto-selection of `test.sql`
- **Tabbed results**: Planned for object lineage, column lineage, and logs display

When modifying the GUI, remember the app uses `threading.Thread` for analysis to keep the UI responsive.
