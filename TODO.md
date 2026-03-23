# Fix Comms Chat ID Integration & /list Empty List

## Step 1: ✅ Get new chat ID
Manual resolve https://t.me/+3jOBEzRLdHI0ZDli → -1003...?

## Step 2: Update config.py (model)
Extend USERS_ADDITIONAL_INFO to include "homework_chat_id"

## Step 3: Update bot.py sheet sync
Load row[1]→homework_chat_id, row[4]→tracker_chat_id

## Step 4: Fix add_user_to_spreadsheet append_row
[email, NEW_COMMS_ID, flow, "", def_tracker, ""]

## Step 5: Add get_users_by_homework_chat_id in database.py

## Step 6: Update /get_tracker_chats_list
Support ?type=homework|tracker param/filter

## Step 7: Manual sheet updates
- tracker_ids sheet: add ["New Comms", NEW_ID] if missing
- users ws0: bulk set col1=NEW_ID for students?

## Step 8: Test
- New user integration → row with col1=new_comms
- /list in new chat → non-empty list
- Both chat_ids used (extend handlers?)"
