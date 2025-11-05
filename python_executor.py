import docker
import tempfile
import textwrap
import os

from dataclasses import dataclass

from dotenv import load_dotenv
load_dotenv()

SERVER_URL = os.getenv("SERVER_URL", "http://localhost:8000")

@dataclass
class Config:
    """Configuration settings"""
    python_project_folder: str = ""
    request_timeout: int = 30
    max_message_length: int = 1024
    docker_image: str = "continuumio/miniconda3"
    docker_memory_limit: str = "128m"

    @classmethod
    def from_env(cls) -> 'Config':
        """Load configuration from environment variables"""
        return cls(
            python_project_folder=os.getenv("PYTHON_PROJECT_FOLDER", ""),
            docker_image=os.getenv("DOCKER_IMAGE", "continuumio/miniconda3"),
        )


class AnalysisExecutor:
    """Execute pandas analysis code in a Docker container for statistical analysis and data operations"""
    def __init__(self, config: Config = None):
        if config is None:
            self.config = Config.from_env()
        else:
            self.config = config

    def analyze_data(self, query_id: str, csv_folder: str, code: str) -> str:
        """
        Execute pandas analysis code on a CSV file using Docker.
        Designed for statistical analysis and data operations with short outputs.
        Examples: df.describe(), df.corr(), df.groupby().mean(), df.value_counts()
        The DataFrame is available as 'df'. Use print() to display results.
        """

        # Check if CSV file exists
        csv_path = os.path.join(csv_folder, f"{query_id}.csv")
        if not os.path.exists(csv_path):
            return f"Error: Query ID '{query_id}' not found (CSV file does not exist)"
        
        # Validate Docker connection
        try:
            client = docker.from_env()
            client.ping()
        except Exception as e:
            return f"Error: Cannot connect to Docker: {str(e)}"
        
        # Validate code
        if not code or not isinstance(code, str):
            return "Error: Code must be a non-empty string"
        code = code.strip()
        if not code:
            return "Error: Code cannot be empty after stripping whitespace"
        
        temp_script = None
        container = None

        try:
            # Prepare script with CSV loading
            script_content = f"""
import pandas as pd
import numpy as np
import traceback

# Load DataFrame from CSV
df = pd.read_csv('/project/{query_id}.csv')

print("DataFrame loaded:", df.shape[0], "rows,", df.shape[1], "columns")
print("=" * 50)

try:
""" + textwrap.indent(textwrap.dedent(code), '    ') + """
except Exception as e:
    print("Error:", str(e))
    traceback.print_exc()
    """

            # Create script file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
                f.write(script_content)
                temp_script = f.name

            # Run container
            container = client.containers.run(
                image=self.config.docker_image,
                command="python /app/script.py",
                volumes={
                    temp_script: {'bind': '/app/script.py', 'mode': 'ro'},
                    csv_folder: {'bind': '/project', 'mode': 'ro'}
                },
                remove=False,
                mem_limit=self.config.docker_memory_limit,
                network_disabled=True,
                detach=True
            )

            wait_result = container.wait(timeout=self.config.request_timeout)

            # Fetch logs
            logs = container.logs(stdout=True, stderr=True)
            output = logs.decode('utf-8', errors='replace') if isinstance(logs, (bytes, bytearray)) else str(logs)

            exit_code = wait_result.get('StatusCode', 0) if isinstance(wait_result, dict) else 0
            if exit_code != 0:
                return f"Container exited with code {exit_code}\n\n{output}"

            return output

        except TimeoutError:
            if container:
                try:
                    container.kill()
                except Exception:
                    pass
                try:
                    container.remove(force=True)
                except Exception:
                    pass
            return f"Execution timed out after {self.config.request_timeout} seconds"

        except Exception as e:
            if container:
                try:
                    container.kill()
                except Exception:
                    pass
                try:
                    container.remove(force=True)
                except Exception:
                    pass
            return f"Execution error: {str(e)}"

        finally:
            if container:
                try:
                    container.remove(force=True)
                except Exception:
                    pass
            if temp_script and os.path.exists(temp_script):
                os.unlink(temp_script)


class MatplotlibExecutor:
    """Execute matplotlib plotting code in a Docker container"""
    def __init__(self, config: Config = None):
        if config is None:
            self.config = Config.from_env()
        else:
            self.config = config

    def create_plot(self, query_id: str, csv_folder: str, plot_code: str) -> dict:
        """Execute matplotlib code to create a plot from CSV file"""

        # Check if CSV file exists
        csv_path = os.path.join(csv_folder, f"{query_id}.csv")
        if not os.path.exists(csv_path):
            return {"error": f"Query ID '{query_id}' not found (CSV file does not exist)"}

        # Validate Docker connection
        try:
            client = docker.from_env()
            client.ping()
        except Exception as e:
            return {"error": f"Cannot connect to Docker: {str(e)}"}

        # Validate code
        if not plot_code or not isinstance(plot_code, str):
            return {"error": "Code must be a non-empty string"}
        plot_code = plot_code.strip()
        if not plot_code:
            return {"error": "Code cannot be empty after stripping whitespace"}

        temp_script = None
        container = None

        try:
            import uuid

            # Generate plot UUID
            plot_id = str(uuid.uuid4())
            plot_filename = f"{plot_id}.png"
            plot_path = os.path.join(csv_folder, plot_filename)

            # Prepare script with CSV loading and plotting
            script_content = f"""
import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import matplotlib.pyplot as plt
import traceback

# Load DataFrame from CSV
df = pd.read_csv('/project/{query_id}.csv')

print("DataFrame loaded:", df.shape[0], "rows,", df.shape[1], "columns")

try:
    # Create new figure
    plt.figure(figsize=(10, 6))

    # Execute user's plotting code
""" + textwrap.indent(textwrap.dedent(plot_code), '    ') + f"""

    # Save plot
    plt.savefig('/project/{plot_filename}', dpi=300, bbox_inches='tight')
    plt.close()
    print("Plot saved successfully: {plot_filename}")

except Exception as e:
    plt.close()  # Cleanup on error
    print("Error:", str(e))
    traceback.print_exc()
    """

            # Create script file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
                f.write(script_content)
                temp_script = f.name

            # Run container
            container = client.containers.run(
                image=self.config.docker_image,
                command="python /app/plot_script.py",
                volumes={
                    temp_script: {'bind': '/app/plot_script.py', 'mode': 'ro'},
                    csv_folder: {'bind': '/project', 'mode': 'rw'}
                },
                remove=False,
                mem_limit=self.config.docker_memory_limit,
                network_disabled=True,
                detach=True
            )

            wait_result = container.wait(timeout=self.config.request_timeout)

            # Fetch logs
            logs = container.logs(stdout=True, stderr=True)
            output = logs.decode('utf-8', errors='replace') if isinstance(logs, (bytes, bytearray)) else str(logs)

            exit_code = wait_result.get('StatusCode', 0) if isinstance(wait_result, dict) else 0
            if exit_code != 0:
                return {"error": f"Container exited with code {exit_code}", "output": output}

            # Check if plot file was created
            if not os.path.exists(plot_path):
                return {"error": "Plot file was not created", "output": output}

            return {
                "plot_download_link": f"{SERVER_URL}/files/{plot_filename}",
                "plot_id": plot_id,
                "filename": plot_filename,
                "message": "Plot created successfully",
                "output": output
            }

        except TimeoutError:
            if container:
                try:
                    container.kill()
                except Exception:
                    pass
                try:
                    container.remove(force=True)
                except Exception:
                    pass
            return {"error": f"Execution timed out after {self.config.request_timeout} seconds"}

        except Exception as e:
            if container:
                try:
                    container.kill()
                except Exception:
                    pass
                try:
                    container.remove(force=True)
                except Exception:
                    pass
            return {"error": f"Execution error: {str(e)}"}

        finally:
            if container:
                try:
                    container.remove(force=True)
                except Exception:
                    pass
            if temp_script and os.path.exists(temp_script):
                os.unlink(temp_script)
