import docker
import tempfile
import textwrap
import os
import pickle

from dataclasses import dataclass

from dotenv import load_dotenv
load_dotenv()

@dataclass
class Config:
    """Configuration settings"""
    python_project_folder: str = ""
    request_timeout: int = 10
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


class PandasExecutor:
    """Execute pandas code in a Docker container"""  
    def __init__(self, config: Config = None):
        if config is None:
            self.config = Config.from_env()
        else:
            self.config = config 

    def execute_code(self, query_id: str, query_cache:dict, code: str) -> str:
        """Execute pandas code on a cached DataFrame using Docker"""
        
        # Get DataFrame from cache
        if query_id not in query_cache:
            return f"Error: Query ID '{query_id}' not found in cache"
        
        df = query_cache[query_id]
        
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
        temp_pickle = None
        
        try:
            # Pickle DataFrame to temp file
            with tempfile.NamedTemporaryFile(mode='wb', suffix='.pkl', delete=False) as f:
                pickle.dump(df, f)
                temp_pickle = f.name
            
            # Prepare script with DataFrame loading
            script_content = """
import pickle
import pandas as pd
import traceback

# Load DataFrame
with open('/project/dataframe.pkl', 'rb') as f:
    df = pickle.load(f)

print("DataFrame loaded:", df.shape[0], "rows,", df.shape[1], "columns")

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
            
            print(f"Executing script:\n{script_content}")
            print("-------------------------------------")
            # Run container
            container = client.containers.run(
                image=self.config.docker_image,
                command="python /app/script.py",
                volumes={
                    temp_script: {'bind': '/app/script.py', 'mode': 'ro'},
                    temp_pickle: {'bind': '/project/dataframe.pkl', 'mode': 'ro'},
                    self.config.python_project_folder: {'bind': '/project', 'mode': 'rw'}
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
            if temp_pickle and os.path.exists(temp_pickle):
                os.unlink(temp_pickle)
