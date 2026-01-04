# Deploying & Sharing DuckEL

To share this POC with your acquaintance, you have two main options.

## Option 1: Instant Sharing (ngrok)
This effectively "tunnels" your currently running local app to a public URL.

1.  **Install ngrok**:
    ```bash
    brew install ngrok/ngrok/ngrok
    ```
2.  **Authenticate** (You need a free account from [ngrok.com](https://ngrok.com)):
    ```bash
    ngrok config add-authtoken <YOUR_TOKEN>
    ```
3.  **Run the tunnel**:
    Assuming your Streamlit app is running on port **8501**:
    ```bash
    ngrok http 8501
    ```
4.  **Share**:
    Copy the `https://....ngrok-free.app` URL shown in the terminal and send it to your friend.

**Note**: This only works while your laptop is on and the app is running.

## Option 2: Persistent Cloud Deployment (Streamlit Community Cloud)
This creates a permanent link that works even when your computer is off.

1.  Push your latest changes to GitHub (completed).
2.  Go to [share.streamlit.io](https://share.streamlit.io/).
3.  Click **"New App"**.
4.  Select this repository (`MrBisonte/quacknettor`).
5.  Set "Main file path" to `app.py`.
6.  **Advanced Settings (Secrets)**:
    Since your friend cannot access your local Postgres, you must configure the app to run in "Demo Mode".
    In the deployment "Secrets" section, you can add your `JULES_API_KEY` if you want them to test AI features.
    
    *Note: The "Demo Local File" pipeline works without Postgres credentials.*

7.  Click **Deploy**.
8.  Send the resulting URL to your friend.

## What Your Friend Will See
They should select **Source: demo_parquet_in** and **Target: parquet_local_out** to run the pipeline successfully without needing database access.
