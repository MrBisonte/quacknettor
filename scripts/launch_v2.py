import subprocess
import time
import sys
import os
import webbrowser
from pathlib import Path

def launch():
    print("🚀 Launching DuckEL v2.0 Platform...")
    
    # Path setup
    root_dir = Path(__file__).parent.parent.absolute()
    venv_python = root_dir / "venv" / "Scripts" / "python.exe"
    node_bin = Path("C:/Program Files/nodejs")
    
    # 1. Start Backend (FastAPI)
    print("\n[1/2] Starting Backend (FastAPI)...")
    backend_proc = subprocess.Popen(
        [str(venv_python), "-m", "uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"],
        cwd=str(root_dir)
    )
    
    # 2. Start Frontend (Next.js)
    print("[2/2] Starting Frontend (Next.js)...")
    env = os.environ.copy()
    env["PATH"] = str(node_bin) + os.pathsep + env["PATH"]
    
    frontend_proc = subprocess.Popen(
        ["npm.cmd", "run", "dev"],
        cwd=str(root_dir / "web"),
        env=env,
        shell=True
    )
    
    print("\n" + "="*40)
    print("✅ System initialized successfully!")
    print("="*40)
    print(f"Backend API:  http://localhost:8000/api")
    print(f"Frontend App: http://localhost:3000")
    print("="*40)
    
    print("\nWaiting for servers to warm up...")
    time.sleep(5)
    
    print("\nOpening Dashboard in your browser...")
    webbrowser.open("http://localhost:3000/dashboard")
    
    print("\nPress Ctrl+C to stop both servers.")
    
    try:
        while True:
            # Check if processes are still running
            if backend_proc.poll() is not None:
                print("Backend stopped unexpectedly.")
                break
            if frontend_proc.poll() is not None:
                print("Frontend stopped unexpectedly.")
                break
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping servers...")
        backend_proc.terminate()
        frontend_proc.terminate()
        print("Goodbye!")

if __name__ == "__main__":
    launch()
