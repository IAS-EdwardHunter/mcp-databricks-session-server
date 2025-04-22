import os
import uuid
from typing import Dict, Optional, Any
from datetime import datetime, timedelta
from dotenv import load_dotenv
from databricks.sql import connect
from databricks.sql.client import Connection
from mcp.server.fastmcp import FastMCP
import requests

# Load environment variables
load_dotenv()

# Set up the MCP server
mcp = FastMCP("Databricks API Explorer")

# Session management
sessions = {}

class Session:
    def __init__(self, host: str, token: str, http_path: str):
        self.id = str(uuid.uuid4())
        self.host = host
        self.token = token
        self.http_path = http_path
        self.created_at = datetime.now()
        self.last_used = datetime.now()
        # Session expires after 1 hour of inactivity
        self.expiry = timedelta(hours=1)
    
    def update_last_used(self):
        self.last_used = datetime.now()
    
    def is_expired(self) -> bool:
        return datetime.now() - self.last_used > self.expiry

def get_session(session_id: str) -> Optional[Session]:
    """Get session by ID and update last used timestamp"""
    session = sessions.get(session_id)
    if not session:
        return None
    
    if session.is_expired():
        # Clean up expired session
        del sessions[session_id]
        return None
    
    # Update last used timestamp
    session.update_last_used()
    return session

# Helper function to get a Databricks SQL connection
def get_databricks_connection(session_id: Optional[str] = None) -> Connection:
    """Create and return a Databricks SQL connection using session or default credentials"""
    if session_id:
        session = get_session(session_id)
        if not session:
            raise ValueError("Invalid or expired session ID")
        
        host = session.host
        token = session.token
        http_path = session.http_path
    else:
        # Fallback to environment variables
        host = os.getenv("DATABRICKS_HOST")
        token = os.getenv("DATABRICKS_TOKEN")
        http_path = os.getenv("DATABRICKS_HTTP_PATH")
    
    if not all([host, token, http_path]):
        raise ValueError("Missing required Databricks connection details")

    return connect(
        server_hostname=host,
        http_path=http_path,
        access_token=token
    )

# Helper function for Databricks REST API requests
def databricks_api_request(endpoint: str, method: str = "GET", data: Dict = None, session_id: Optional[str] = None) -> Dict:
    """Make a request to the Databricks REST API using session or default credentials"""
    if session_id:
        session = get_session(session_id)
        if not session:
            raise ValueError("Invalid or expired session ID")
        
        host = session.host
        token = session.token
    else:
        # Fallback to environment variables
        host = os.getenv("DATABRICKS_HOST")
        token = os.getenv("DATABRICKS_TOKEN")
    
    if not all([host, token]):
        raise ValueError("Missing required Databricks API credentials")
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    url = f"https://{host}/api/2.0/{endpoint}"
    
    if method.upper() == "GET":
        response = requests.get(url, headers=headers)
    elif method.upper() == "POST":
        response = requests.post(url, headers=headers, json=data)
    else:
        raise ValueError(f"Unsupported HTTP method: {method}")
    
    response.raise_for_status()
    return response.json()

# Authentication tools
@mcp.tool()
def login(host: str, token: str, http_path: str) -> str:
    """
    Login to Databricks and create a new session
    
    Args:
        host: Databricks host URL (e.g., 'adb-123456789.0.azuredatabricks.net')
        token: Databricks access token
        http_path: HTTP path for SQL warehouse (e.g., 'sql/protocolv1/o/123456789/0123-456789-abcdef01')
    """
    try:
        # Validate credentials by attempting to connect
        conn = connect(
            server_hostname=host,
            http_path=http_path,
            access_token=token
        )
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        conn.close()
        
        # Create new session
        session = Session(host, token, http_path)
        sessions[session.id] = session
        
        return f"""
## Successfully authenticated with Databricks

**Host**: {host}
**Session ID**: {session.id}

Your session will expire after 1 hour of inactivity.
Please use this session_id in subsequent requests.
        """
    except Exception as e:
        return f"Authentication failed: {str(e)}"

@mcp.tool()
def logout(session_id: str) -> str:
    """
    Logout and destroy a Databricks session
    
    Args:
        session_id: Session ID from a previous login
    """
    if session_id in sessions:
        del sessions[session_id]
        return "Successfully logged out. Your session has been destroyed."
    return "Session not found. It may have expired or been invalid."

@mcp.tool()
def session_status(session_id: str) -> str:
    """
    Check the status of a Databricks session
    
    Args:
        session_id: Session ID from a previous login
    """
    session = get_session(session_id)
    if not session:
        return "Session not found or expired. Please login again."
    
    expires_in = session.expiry - (datetime.now() - session.last_used)
    minutes = int(expires_in.total_seconds() / 60)
    
    return f"""
## Session Status

**Session ID**: {session.id}
**Host**: {session.host}
**Created**: {session.created_at.strftime('%Y-%m-%d %H:%M:%S')}
**Expires in**: {minutes} minutes

Your session is active.
    """

# Modify existing tools to use session_id
@mcp.resource("schema://tables")
def get_schema(session_id: Optional[str] = None) -> str:
    """
    Provide the list of tables in the Databricks SQL warehouse as a resource
    
    Args:
        session_id: (Optional) Session ID from a previous login
    """
    try:
        conn = get_databricks_connection(session_id)
        cursor = conn.cursor()
        tables = cursor.tables().fetchall()
        
        table_info = []
        for table in tables:
            table_info.append(f"Database: {table.TABLE_CAT}, Schema: {table.TABLE_SCHEM}, Table: {table.TABLE_NAME}")
        
        return "\n".join(table_info)
    except Exception as e:
        return f"Error retrieving tables: {str(e)}"
    finally:
        if 'conn' in locals():
            conn.close()

@mcp.tool()
def run_sql_query(sql: str, session_id: Optional[str] = None) -> str:
    """
    Execute SQL queries on Databricks SQL warehouse
    
    Args:
        sql: SQL query to execute
        session_id: (Optional) Session ID from a previous login
    """
    try:
        conn = get_databricks_connection(session_id)
        cursor = conn.cursor()
        result = cursor.execute(sql)
        
        if result.description:
            # Get column names
            columns = [col[0] for col in result.description]
            
            # Format the result as a table
            rows = result.fetchall()
            if not rows:
                return "Query executed successfully. No results returned."
            
            # Format as markdown table
            table = "| " + " | ".join(columns) + " |\n"
            table += "| " + " | ".join(["---" for _ in columns]) + " |\n"
            
            for row in rows:
                table += "| " + " | ".join([str(cell) for cell in row]) + " |\n"
                
            return table
        else:
            return "Query executed successfully. No results returned."
    except Exception as e:
        return f"Error executing query: {str(e)}"
    finally:
        if 'conn' in locals():
            conn.close()

@mcp.tool()
def list_jobs(session_id: Optional[str] = None) -> str:
    """
    List all Databricks jobs
    
    Args:
        session_id: (Optional) Session ID from a previous login
    """
    try:
        response = databricks_api_request("jobs/list", session_id=session_id)
        
        if not response.get("jobs"):
            return "No jobs found."
        
        jobs = response.get("jobs", [])
        
        # Format as markdown table
        table = "| Job ID | Job Name | Created By |\n"
        table += "| ------ | -------- | ---------- |\n"
        
        for job in jobs:
            job_id = job.get("job_id", "N/A")
            job_name = job.get("settings", {}).get("name", "N/A")
            created_by = job.get("created_by", "N/A")
            
            table += f"| {job_id} | {job_name} | {created_by} |\n"
        
        return table
    except Exception as e:
        return f"Error listing jobs: {str(e)}"

# Update get_job_status and get_job_details similarly