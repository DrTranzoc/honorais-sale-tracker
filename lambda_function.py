from datetime import datetime
import json
import boto3
import requests
import os
import time

session = boto3.Session(
    region_name='eu-north-1',
)

dynamodb = session.resource('dynamodb')

tool_settings = dynamodb.Table('honorais-tool-status')
subcriptions_table = dynamodb.Table('honorais-tool-subcriptions')
nft_data_table = dynamodb.Table('nft-metadata')

TOOL_SETTINGS = {}

def lambda_handler(event, context):
    
    global TOOL_SETTINGS
    TOOL_SETTINGS = tool_settings.get_item(Key={"tool_version" : os.getenv("TOOL_VERSION")})['Item']
    
    #Retrieve every tracker subscription
    subscriptions = scan_table(subcriptions_table)
    
    collections_to_track = {track["collection_address"] for data in subscriptions for track in data["sales_tracker_settings"] if data["enabled"]}

    #Foreach subscription, send the notification to target guild -> channel, if the collection match
    for current_collection in collections_to_track:
        try:
            reachedBottom = False
            last_tx = TOOL_SETTINGS["sales_tracking_status"][current_collection] if current_collection in TOOL_SETTINGS["sales_tracking_status"] else ""

            new_last_tx = ""

            base_url = os.getenv("ORAI_RPC") + current_collection
            final_url = base_url 
            
            page = 0
            while not reachedBottom:
                page += 1
                final_url = base_url + f"?page={str(page)}"
                
                #Scan last 100 Txs
                transactions = requests.get(final_url).json()["data"]
                relevant_transactions = []
                
                #Delete transactions older than the last
                
                for transaction in transactions:
                    if transaction["tx_hash"] == last_tx:
                        reachedBottom = True
                        break
                    relevant_transactions.append(transaction)
                
                relevant_transactions.reverse()

                for transaction in relevant_transactions:
                    
                    new_last_tx = transaction["tx_hash"]

                    for message in transaction["messages"]:
                        if "buy_token" not in message["msg"]:
                            continue
                        
                        buy_data = message["msg"]["buy_token"]
                        
                        token_id = buy_data["token_id"]
                        amount = message["funds"][0]["amount"]
                        token_name = message["funds"][0]["denom"]
                        buyer = message["sender"]

                        nft_data = nft_data_table.get_item(Key={
                            "collection_address" : current_collection,
                            "token_id" : token_id
                        })['Item']

                        media = nft_data["media"]
                        name = nft_data["attributes"]["title"]

                        fields = [
                            {
                                "name" : "**NFT**",
                                "value" : name,
                                "inline" : True
                            },
                            {
                                "name" : "**Price**",
                                "value" : f"{int(amount)/1000000} {token_name}",
                                "inline" : True
                            },
                            {
                                "name" : "**WEN**",
                                "value" : f"<t:{int(datetime.strptime(transaction["timestamp"], "%Y-%m-%dT%H:%M:%SZ").timestamp())}>",
                                "inline" : True
                            },
                            {
                                "name" : "**Buyer**",
                                "value" : buyer,
                                "inline" : True
                            }
                        ]

                        embed =  build_embed(
                            title=f"NEW SALE {name}",
                            color=5763719,
                            description=f"New salle occurred!",
                            fields=fields,
                            image_url=media.replace("#","%23"),
                            footer_text="Built by @honorais\n"
                        )

                        message = {
                            "content" : "",
                            "embeds" : [embed]
                        }
                        
                        send_messages(current_collection , subscriptions , message)
                        
                        time.sleep(1)
                
                #If the collection has never been tracked before
                if last_tx == "":
                    break
            
            if new_last_tx != "":
                TOOL_SETTINGS["sales_tracking_status"][current_collection] = new_last_tx
                tool_settings.put_item(Item=TOOL_SETTINGS)

        except Exception as e:
            print(f"ERROR during {current_collection} fetch : {str(e)}")

    return {
        'statusCode': 200,
        'body': json.dumps('Done')
    }
    
def send_messages(current_collection : str, subscriptions, message):
    for subscription in subscriptions:
        
        for tracked_collection in subscription["sales_tracker_settings"]:
            
            if tracked_collection["collection_address"] != current_collection:
                continue

            channel_id = tracked_collection["channel_id"]
            
            dispatch_notification(channel_id , message)
                
                
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
        "fields": []
    }

    for field in fields:
        embed["fields"].append({
            "name": field["name"],
            "value": field["value"],
            "inline": field.get("inline", False)
        })

    if footer_text or footer_icon_url:
        embed["footer"] = {
            "text": footer_text,
            "icon_url": footer_icon_url
        }

    if image_url:
        embed["image"] = {
            "url": image_url
        }

    return embed


def scan_table(table):
    scan_kwargs = {}
    done = False
    start_key = None
    items = []

    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key
        response = table.scan(**scan_kwargs)
        items.extend(response.get('Items', []))
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None

    return items