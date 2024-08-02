from datetime import datetime
import json
import boto3
import requests
import os
import time

session = boto3.Session(region_name='eu-north-1')
dynamodb = session.resource('dynamodb')

tool_settings_table = dynamodb.Table('honorais-tool-status')
subscriptions_table = dynamodb.Table('honorais-tool-subcriptions')
nft_data_table = dynamodb.Table('nft-metadata')

TOOL_SETTINGS = {}

def lambda_handler(event, context):
    global TOOL_SETTINGS
    TOOL_SETTINGS = get_tool_settings()

    subscriptions = scan_table(subscriptions_table)
    collections_to_track = get_collections_to_track(subscriptions)

    for current_collection in collections_to_track:
        process_collection(current_collection, subscriptions)

    return {
        'statusCode': 200,
        'body': json.dumps('Done')
    }

def get_tool_settings():
    return tool_settings_table.get_item(Key={"tool_version": os.getenv("TOOL_VERSION")})['Item']

def get_collections_to_track(subscriptions):
    return {track["collection_address"] for data in subscriptions for track in data["sales_tracker_settings"] if data["enabled"]}

def process_collection(current_collection, subscriptions):
    try:
        last_tx = TOOL_SETTINGS["sales_tracking_status"].get(current_collection, "")
        new_last_tx = track_transactions(current_collection, last_tx, subscriptions)

        if new_last_tx:
            TOOL_SETTINGS["sales_tracking_status"][current_collection] = new_last_tx
            tool_settings_table.put_item(Item=TOOL_SETTINGS)
    except Exception as e:
        print(f"ERROR during {current_collection} fetch: {str(e)}")

def track_transactions(current_collection, last_tx, subscriptions):
    reached_bottom = False
    new_last_tx = ""
    base_url = os.getenv("ORAI_RPC") + current_collection

    page = 0
    while not reached_bottom:
        page += 1
        transactions = get_transactions(base_url, page)
        relevant_transactions = filter_transactions(transactions, last_tx)

        for transaction in relevant_transactions:
            new_last_tx = transaction["tx_hash"]
            process_transaction(transaction, current_collection, subscriptions)

        if not relevant_transactions or last_tx == "":
            break

    return new_last_tx

def get_transactions(base_url, page):
    final_url = f"{base_url}?page={str(page)}"
    response = requests.get(final_url)
    response.raise_for_status()
    return response.json()["data"]

def filter_transactions(transactions, last_tx):
    relevant_transactions = []
    for transaction in transactions:
        if transaction["tx_hash"] == last_tx:
            break
        relevant_transactions.append(transaction)
    relevant_transactions.reverse()
    return relevant_transactions

def process_transaction(transaction, current_collection, subscriptions):
    for message in transaction["messages"]:
        if "buy_token" not in message["msg"]:
            continue

        buy_data = message["msg"]["buy_token"]
        nft_data = get_nft_data(current_collection, buy_data["token_id"])

        fields = build_fields(transaction, buy_data, nft_data)
        embed = build_embed(
            title=f"NEW SALE {nft_data['attributes']['title']}",
            color=5763719,
            description="New sale occurred!",
            fields=fields,
            image_url=nft_data["media"].replace("#", "%23"),
            footer_text="Built by @honorais\n"
        )

        message = {"content": "", "embeds": [embed]}
        send_messages(current_collection, subscriptions, message)
        time.sleep(1)

def get_nft_data(current_collection, token_id):
    return nft_data_table.get_item(Key={"collection_address": current_collection, "token_id": token_id})['Item']

def build_fields(transaction, buy_data, nft_data):
    return [
        {"name": "**NFT**", "value": nft_data["attributes"]["title"], "inline": True},
        {"name": "**Price**", "value": f"{int(buy_data['amount']) / 1000000} {buy_data['denom']}", "inline": True},
        {"name": "**WEN**", "value": f"<t:{int(datetime.strptime(transaction['timestamp'], '%Y-%m-%dT%H:%M:%SZ').timestamp())}>", "inline": True},
        {"name": "**Buyer**", "value": buy_data["sender"], "inline": True}
    ]

def send_messages(current_collection, subscriptions, message):
    for subscription in subscriptions:
        for tracked_collection in subscription["sales_tracker_settings"]:
            if tracked_collection["collection_address"] == current_collection:
                dispatch_notification(tracked_collection["channel_id"], message)

def dispatch_notification(channel_id, message):
    url = f'https://discord.com/api/v10/channels/{channel_id}/messages'
    headers = {
        'Authorization': f'Bot {os.getenv("BOT_TOKEN")}',
        'Content-Type': 'application/json'
    }
    response = requests.post(url, headers=headers, json=message)
    if response.status_code == 200:
        print('Embed sent successfully!')
    else:
        print(f'Failed to send embed: {response.status_code} - {response.text}')

def build_embed(title, description, color, fields, footer_text=None, footer_icon_url=None, image_url=None):
    embed = {
        "title": title,
        "description": description,
        "color": color,
        "fields": [{"name": field["name"], "value": field["value"], "inline": field.get("inline", False)} for field in fields]
    }
    if footer_text or footer_icon_url:
        embed["footer"] = {"text": footer_text, "icon_url": footer_icon_url}
    if image_url:
        embed["image"] = {"url": image_url}
    return embed

def scan_table(table):
    items = []
    start_key = None
    while True:
        scan_kwargs = {'ExclusiveStartKey': start_key} if start_key else {}
        response = table.scan(**scan_kwargs)
        items.extend(response.get('Items', []))
        start_key = response.get('LastEvaluatedKey', None)
        if not start_key:
            break
    return items
