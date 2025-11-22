import json
import boto3
import csv
import io
from datetime import datetime

# --- CONFIGURATION ---
# ⚠️ แก้บรรทัดนี้: เอา ARN ที่ได้จาก Step 1 มาใส่ในเครื่องหมายคำพูด
SNS_TOPIC_ARN = 'วาง-ARN-ของ-SNS-ที่นี่' 
DYNAMODB_TABLE = 'ExpenseTracker'

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    dynamodb = boto3.resource('dynamodb')
    sns = boto3.client('sns')
    table = dynamodb.Table(DYNAMODB_TABLE)
    
    # 1. รับข้อมูลจาก S3 Trigger
    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
    except KeyError:
        return {'statusCode': 400, 'body': 'Error: No S3 event data found.'}
    
    # เราจะประมวลผลเฉพาะไฟล์ .csv เท่านั้น
    if not key.endswith('.csv'):
        return {'statusCode': 200, 'body': 'Not a CSV file, skipping.'}
    
    print(f"Processing CSV: {key} from {bucket}")
    
    # 2. อ่านไฟล์ Budget Limit (budget.json) จาก Bucket เดียวกัน
    try:
        obj = s3.get_object(Bucket=bucket, Key='budget.json')
        budget_data = json.loads(obj['Body'].read().decode('utf-8'))
        budget_limit = int(budget_data.get('limit', 10000)) # ถ้าไม่มีไฟล์ ให้ default 10000
    except:
        budget_limit = 10000
        print("Budget file not found, using default 10,000")

    # 3. อ่านและรวมเงินจาก CSV
    try:
        csv_obj = s3.get_object(Bucket=bucket, Key=key)
        content = csv_obj['Body'].read().decode('utf-8')
        
        total_amount = 0
        csv_reader = csv.reader(io.StringIO(content))
        next(csv_reader, None) # ข้าม Header
        for row in csv_reader:
            if row and len(row) >= 3:
                try:
                    total_amount += float(row[2]) # ยอดเงินอยู่คอลัมน์ที่ 3
                except ValueError:
                    continue # ข้ามแถวที่แปลงตัวเลขไม่ได้
    except Exception as e:
        return {'statusCode': 500, 'body': f"Error reading CSV: {str(e)}"}
            
    # 4. อัปเดตลง DynamoDB
    current_month = datetime.now().strftime('%Y-%m')
    try:
        response = table.get_item(Key={'Month': current_month})
        current_balance = float(response['Item']['Total']) if 'Item' in response else 0
    except:
        current_balance = 0
        
    new_balance = current_balance + total_amount
    table.put_item(Item={'Month': current_month, 'Total': str(new_balance)})
    
    # 5. แจ้งเตือนถ้าเกินงบ
    msg = f"Updated! Used: {new_balance} / Budget: {budget_limit}"
    if new_balance > budget_limit:
        alert_msg = f"🚨 ALERT: Budget Exceeded! Used: {new_balance} THB (Limit: {budget_limit})"
        sns.publish(TopicArn=SNS_TOPIC_ARN, Message=alert_msg, Subject='Budget Alert!')
        msg = alert_msg
        
    return {'statusCode': 200, 'body': msg}
