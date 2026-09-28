# 🚀 Meta Webhook App Setup Instructions

## 1. Refreshing the API Token

⚠️ If you’re still using a **temporary token**:

* You must refresh it daily via **Meta Developer Dashboard → Tools → Access Tokens**.

To obtain a permanent access token:

1. Go to **Meta Developer Dashboard → Business Settings → System Users**.
2. Create a **System User** (if not already created).
3. Generate a **permanent access token** tied to that user.
4. Give it the role **WhatsApp Business API → Full Access**.
5. Copy the new token to your `.env` file (see Step 2).
6. Restart FastAPI + ngrok.

---

## 2. Environment Setup

* Make sure your environment variables are set in `.env`:
  ```bash
  WA_NUMBER_ID=<whatsapp_number_id>    # no quotes
  WA_TOKEN=<whatsapp_access_token>     # no quotes
  WA_VERIFY_TOKEN=<"my_secret_verify"> # double quotes
  ```
* Load the env vars:
  ```bash
  source .env
  ```
* ✅ Check validity:
  ```bash
  echo $WA_NUMBER_ID
  echo $WA_TOKEN
  echo $WA_VERIFY_TOKEN
  ```

⚠️ If your **WhatsApp API access token** was temporary (1 day), refresh it in the Meta Developer dashboard. Use a **permanent token** when ready.

---

## 3. Start Ngrok

In a new terminal:
```bash
ngrok http 5000
```

After launch, copy the **HTTPS forwarding URL**, e.g., `https://abc123.ngrok-free.app`.

---

## 4. Launch FastAPI Webhook

Start the Python script:
```bash
python3 demo_webhook.py
```

You should see:
```
INFO: Uvicorn running on http://127.0.0.1:5000 (Press CTRL+C to quit)
```

---

## 5. Configure Meta Webhook

1. Go to **Meta Developer Dashboard → App Name → WhatsApp → Configuration**.
2. In **Callback URL**, paste the ngrok URL + `/webhook`, for example:
   ```
   https://abc123.ngrok-free.app/webhook
   ```
3. In **Verify Token**, enter the value you set in your environment (`my_secret_verify`), without the quotes.
4. Click **Verify and Save**.

---

## 6. Subscribe to Webhook Fields

* Under **Webhook Fields**, enable:
  * ✅ `messages`

---

## 7. Testing

* Add your personal WhatsApp number as a test user in the app.
* Send a text or image to the test number.
* The script should show the incoming JSON.

---

## 8. Stopping

* Stop FastAPI: `Ctrl+C`
* Stop ngrok: `Ctrl+C`

---
