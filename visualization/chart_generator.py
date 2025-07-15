import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from typing import Optional, Dict, Any
import logging
import os
from langchain.chains import LLMChain
from langchain_core.prompts import PromptTemplate
from langchain_groq import ChatGroq
from dotenv import load_dotenv
import re

# Load environment variables
load_dotenv()

class ChartGenerator:
    """Generate Python code for data visualization based on natural language prompts."""

    def __init__(self):
        self.llm = ChatGroq(
            model_name="llama3-70b-8192",
            groq_api_key=os.getenv("GROQ_API_KEY"),
            temperature=0.1
        )
        self.visualization_prompt = self._get_visualization_prompt()

    def _get_visualization_prompt(self) -> str:
        """Get the prompt template for generating visualization code."""
        return """
You are an expert Python data visualization developer. Generate Python code to create a chart based on the user's request.

Dataset Information:
- DataFrame variable name: df
- Columns: {columns}
- Data types: {dtypes}
- Sample data: {sample_data}
- Dataset shape: {shape}

User Query: {query}

Requirements:
1. Use plotly.express (px) or plotly.graph_objects (go) for visualization
2. The DataFrame is already loaded as 'df'
3. Import statements should be: import plotly.express as px, import plotly.graph_objects as go
4. Return a plotly figure object assigned to variable 'fig'
5. Add appropriate titles, labels, and styling
6. Handle missing values appropriately
7. If data needs aggregation, include the necessary pandas operations
8. Use appropriate color schemes and layouts
9. Include hover information where relevant
10. Make the chart interactive and professional-looking
11. Do not use unsupported layout keys like 'hover' or 'hover_data'

Chart Type Guidelines:
- For time series: Use line charts with markers
- For comparisons: Use bar charts or grouped bar charts
- For distributions: Use histograms or box plots
- For relationships: Use scatter plots with trendlines
- For proportions: Use pie charts or donut charts
- For multiple variables: Use subplots or faceted charts

Return ONLY the Python code without any markdown formatting or explanations.
The code should be executable and create a complete visualization.

Python Code:
"""

    def generate_chart_code(self, df: pd.DataFrame, query: str) -> str:
        try:
            analysis = self._analyze_dataframe(df)
            prompt = PromptTemplate(
                input_variables=["columns", "dtypes", "sample_data", "shape", "query"],
                template=self.visualization_prompt
            )
            chain = LLMChain(llm=self.llm, prompt=prompt)
            code_response = chain.run({
                "columns": analysis["columns"],
                "dtypes": analysis["dtypes"],
                "sample_data": analysis["sample_data"],
                "shape": analysis["shape"],
                "query": query
            })
            cleaned_code = self._clean_code(code_response)
            return cleaned_code
        except Exception as e:
            logging.error(f"Chart code generation failed: {str(e)}")
            return self._generate_fallback_code(df, query)

    def execute_chart_code(self, df: pd.DataFrame, code: str) -> Optional[go.Figure]:
        try:
            exec_globals = {
                'df': df,
                'pd': pd,
                'px': px,
                'go': go,
                'np': np,
                'make_subplots': make_subplots
            }
            exec(code, exec_globals)
            if 'fig' in exec_globals:
                return exec_globals['fig']
            raise ValueError("Generated code did not create a 'fig' variable")
        except Exception as e:
            logging.error(f"Code execution failed: {str(e)}")
            return None

    def generate_chart(self, df: pd.DataFrame, query: str = "") -> Optional[go.Figure]:
        if df.empty:
            return None
        try:
            code = self.generate_chart_code(df, query)
            return self.execute_chart_code(df, code)
        except Exception as e:
            logging.error(f"Chart generation failed: {str(e)}")
            return None

    def _analyze_dataframe(self, df: pd.DataFrame) -> Dict[str, Any]:
        analysis = {
            'columns': list(df.columns),
            'dtypes': df.dtypes.astype(str).to_dict(),
            'shape': df.shape,
            'sample_data': df.head(3).to_dict('records'),
            'numeric_cols': df.select_dtypes(include=[np.number]).columns.tolist(),
            'categorical_cols': df.select_dtypes(include=['object']).columns.tolist(),
            'datetime_cols': df.select_dtypes(include=['datetime64']).columns.tolist()
        }
        return analysis

    def _clean_code(self, code_response: str) -> str:
        code_response = code_response.strip()

        # Extract only code if inside markdown
        if '```python' in code_response:
            match = re.search(r'```python\s*(.*?)\s*```', code_response, re.DOTALL)
            if match:
                code_response = match.group(1)
        elif '```' in code_response:
            match = re.search(r'```\s*(.*?)\s*```', code_response, re.DOTALL)
            if match:
                code_response = match.group(1)

        lines = code_response.split('\n')
        cleaned_lines = []
        for line in lines:
            if 'layout=' in line and 'hover=' in line:
                continue  # remove bad layout lines
            if 'hover=' in line or 'hover_data=' in line:
                continue  # remove invalid hover properties
            cleaned_lines.append(line)

        cleaned_code = '\n'.join(cleaned_lines)
        if 'import' not in cleaned_code:
            cleaned_code = (
                "import plotly.express as px\n"
                "import plotly.graph_objects as go\n"
                "import pandas as pd\n"
                "import numpy as np\n\n"
            ) + cleaned_code

        return cleaned_code

    def _generate_fallback_code(self, df: pd.DataFrame, query: str) -> str:
        analysis = self._analyze_dataframe(df)
        if analysis['datetime_cols'] and analysis['numeric_cols']:
            x, y = analysis['datetime_cols'][0], analysis['numeric_cols'][0]
            return f"""
import plotly.express as px
fig = px.line(df, x='{x}', y='{y}', title='{y} over time')
"""
        elif analysis['categorical_cols'] and analysis['numeric_cols']:
            x, y = analysis['categorical_cols'][0], analysis['numeric_cols'][0]
            return f"""
import plotly.express as px
df_grouped = df.groupby('{x}')[['{y}']].sum().reset_index()
fig = px.bar(df_grouped, x='{x}', y='{y}', title='{y} by {x}')
"""
        elif len(analysis['numeric_cols']) >= 2:
            x, y = analysis['numeric_cols'][:2]
            return f"""
import plotly.express as px
fig = px.scatter(df, x='{x}', y='{y}', title='{y} vs {x}')
"""
        else:
            return """
import plotly.graph_objects as go
fig = go.Figure(data=[go.Table(
    header=dict(values=list(df.columns), fill_color='paleturquoise', align='left'),
    cells=dict(values=[df[col] for col in df.columns], fill_color='lavender', align='left')
)])
"""

    def _create_empty_chart(self) -> go.Figure:
        fig = go.Figure()
        fig.add_annotation(
            text="No data to display",
            xref="paper", yref="paper", x=0.5, y=0.5,
            showarrow=False, font=dict(size=16)
        )
        fig.update_layout(title="No Data Available")
        return fig

    def _create_error_chart(self, error_msg: str) -> go.Figure:
        fig = go.Figure()
        fig.add_annotation(
            text=f"Error generating chart: {error_msg}",
            xref="paper", yref="paper", x=0.5, y=0.5,
            showarrow=False, font=dict(size=14, color="red")
        )
        fig.update_layout(title="Chart Generation Error")
        return fig

    def get_generated_code(self, df: pd.DataFrame, query: str) -> str:
        return self.generate_chart_code(df, query)

    def explain_chart_code(self, code: str, query: str) -> str:
        try:
            explanation_template = """
Explain this data visualization code in simple terms for a business user:

Original Request: {query}
Python Code: {code}

Explain:
1. Chart type
2. Data being shown
3. Insights user can get
4. Special features

Keep the explanation non-technical.

Explanation:
"""
            prompt = PromptTemplate(
                input_variables=["query", "code"],
                template=explanation_template
            )
            chain = LLMChain(llm=self.llm, prompt=prompt)
            return chain.run({"query": query, "code": code})
        except Exception as e:
            logging.error(f"Code explanation failed: {str(e)}")
            return f"This code creates a visualization to answer: '{query}'"
