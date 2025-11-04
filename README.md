# Healthcare Agent

A conversational AI assistant for querying and analyzing synthetic patient healthcare data. This agent uses OpenAI's API with function calling to interact with a SQLite database containing Synthea-generated synthetic patient records.

## Installation

1. Clone this repository:
```bash
git clone <repository-url>
cd healthcare-agent
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create a `.env` file in the project root with your OpenAI API key:
```bash
OPENAI_API_KEY=your_api_key_here
```

4. Load the Synthea CSV data into SQLite (if not already done):
```bash
python load_sqlite.py
```

This will create `synthea_data.db` from the CSV files in the `csvs/` directory.

### Basic Usage

Run the chat interface with the default model (gpt-5):
```bash
python chat.py
python chat.py --model gpt-4
python chat.py --model gpt-3.5-turbo
python chat.py --model gpt-4o-mini
```

## Available Tools

The agent has access to the following tools:

1. **query_db**: Execute read-only SQL SELECT queries
2. **get_db_schema**: Retrieve the schema of all database tables
3. **get_patient_data**: Get patient data from various tables (demographics, medications, labs, imaging, procedures, conditions, encounters, allergies, immunizations, careplans, devices)


