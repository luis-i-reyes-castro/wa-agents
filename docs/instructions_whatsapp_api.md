# 📱 WhatsApp Cloud API Setup

## 1. Setup Credentials File

In our `.env` file:
```bash
# WhatsApp Cloud API
WA_BA_NUM=...    # WhatsApp Business Account ID (not a phone number)
WA_TOKEN=EAAB... # WhatsApp Access Token
```

Load the environment variables:
```bash
source .env
```

## 2. Get Phone Number ID from WABA ID

Each WhatsApp Business Account (WABA) can have one or more phone numbers.
To fetch them:
```bash
curl -i -X GET \
  "https://graph.facebook.com/v23.0/$WA_BA_NUM/phone_numbers" \
  -H "Authorization: Bearer $WA_TOKEN"
```

Example response:
```json
{
  "data": [
    {
      "verified_name": "...",
      "display_phone_number": "...",
      "id": "..."
    }
  ]
}
```

Take note of the `"id"` field because this is your **Phone Number ID**.
Add it to your `.env` file, along with a 6-digit PIN of your chosing:
```bash
WA_NUMBER_ID=...
WA_NUMBER_PIN=123456 # Example of 6-digit PIN you chose for registration
```

Reload the environment variables: `source .env`

## 3. Registration Flow

After adding the phone number in **WhatsApp Manager**, it shows as either Online or **Pending**. Either way, we need to complete the number's registration via API:

```bash
curl -i -X POST \
  "https://graph.facebook.com/v23.0/$WA_NUMBER_ID/register" \
  -H "Authorization: Bearer $WA_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "messaging_product": "whatsapp",
    "pin": "'"$WA_NUMBER_PIN"'"
  }'
```

* ⚠️ Use the **`"'"` quotes** trick to expand the env vars.
  * Necessary so that Bash expands `$WA_NUMBER_PIN`
  * Bash **cannot expand** variables inside single quotes!
* On success: `{"success":true}`
* Number status flips to **Connected**.

### If You Cannot Text the Number via WhatsApp

If while trying to find the number on your phone's WhatsApp app you see that it does not have an account, i.e., it only shows the option to **"Invite to WhatsApp"** then you must **send a template message** (to any user) to force the app to recognize the number. See the template example in the following section.

### De-registration

If we later need to get rid of that phone number:
```bash
curl -i -X POST \
  "https://graph.facebook.com/v23.0/$WA_NUMBER_ID/deregister" \
  -H "Authorization: Bearer $WA_TOKEN"
```

* On success: `{"success":true}`
* Number status flips to **Offline**.

## 4. Messaging Rules

* If the **user messages the chatbot first,** the chatbot can reply with free-form text for a **24-hour window**, any number of times.
* If the chatbot must start the conversation, use a **template** (see below).

### Example: template

Go to **WhatsApp Manager → Message templates → Create template**.
Create something simple like:
* **Name:** hello_world (lowercase + underscores only)
* **Category:** UTILITY (or MARKETING, it does not matter)
* **Body:** "Hola! Este es un mensaje de prueba."

Submit and wait for approval.

> Until it’s approved, you can’t use it to start conversations reliably.

Check approval:
```bash
curl -s -X GET \
  "https://graph.facebook.com/v23.0/$WA_BA_NUM/message_templates?limit=50" \
  -H "Authorization: Bearer $WA_TOKEN" | jq
```

If approved, be mindful of the language code (e.g., `en_US` or `es_EC`), as it will need to match the one below. Once approved, add environment variable `TEST_NUMBER` with a test phone number (without the `+`prefix), reload the env vars (`source .env`), and run:
```bash
curl -i -X POST \
  "https://graph.facebook.com/v23.0/$WA_NUMBER_ID/messages" \
  -H "Authorization: Bearer $WA_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "messaging_product": "whatsapp",
    "to": "'"$TEST_NUMBER"'",
    "type": "template",
    "template": {
      "name": "hello_world",
      "language": { "code": "en_US" }
    }
  }'
```

### Example: free-form text

```bash
curl -i -X POST \
  "https://graph.facebook.com/v23.0/$WA_NUMBER_ID/messages" \
  -H "Authorization: Bearer $WA_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "messaging_product": "whatsapp",
    "to": "'"$TEST_NUMBER"'",
    "type": "text",
    "text": { "body": "Hello World!" }
  }'
```

## 5. Limits & Quotas

* Registration API: **10 attempts per 72 hours**.
* Messaging tier: default is **250 customers / 24 hours**.

## 6. ⚡ Quick Troubleshooting Guide

**If dashboard won’t let you enable PIN:**
* Dumb, but totally normal.
* PIN is set during registration (see above).

**If registration fails:**
* Check quotes after `-d`. Use double quotes.
  * In `curl`, single quotes prevent variable expansion!
  * Use double quotes for vars like `$WA_NUMBER_PIN`.
* Confirm PIN is 6 digits.
* Ensure you didn’t exceed **10 register attempts / 72h**.

**If number shows up on the app as not having an account:**
* I.e., only option available is to "Invite to WhatsApp".
* Follow the template example from Section 4.

**If message not delivered (API returned 200):**
* Ensure recipient phone number's in E.164 format (no `+` prefix).
* Did the user message your number first? (if not, use a template).
* Are you within the 24-hour session window?

**If escalation is needed:**
* Support category → *Dev: Phone Number & Registration*.
* Request Type → *Registration Issues*.
* Provide your last failing `curl` call + raw API response.
