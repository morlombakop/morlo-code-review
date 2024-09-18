import json
import boto3
import os
import re
import fitz
import time
import requests
from copy import deepcopy

# AWS-Client-Initialisierungen
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
dynamodb_client = boto3.client("dynamodb")


def convert_pdf(api_url, pdf_file_path):
    try:
        with open(pdf_file_path, 'rb') as file:
            files = {'file': (pdf_file_path, file, 'application/pdf'), 'highlightLinks': ('', "true")}
            start_time = time.time()  # Startzeit messen
            response = requests.post(api_url, files=files)
            end_time = time.time()  # Endzeit messen
            elapsed_time = end_time - start_time  # Berechnung der verstrichenen Zeit

            if response.status_code == 200:
                print("PDF conversion successful. Time taken:", elapsed_time, "seconds")
                return response.content
            else:
                print("Failed to convert PDF. Status code:", response.status_code)
                print("Reason :", response.reason)
                return None
    except Exception as e:
        print("Error:", str(e))
        return None


def getStageVariables(stage):
    if stage == "prod":
        src_bucket = "adb-s3-full-texts-anonymized-prod"
        dstn_bucket = "adb-s3-full.json.segmented-prod"
        all_data_table = 'iurcrowd-adb-alldata-prod'
        output_bucket = 'iurcrowd-s3-lawlinks-prod'
    else:
        src_bucket = "adb-s3-full-texts-anonymized-dev"
        dstn_bucket = "adb-s3-full.json.segmented-dev"
        all_data_table = 'iurcrowd-adb-alldata-dev'
        output_bucket = 'iurcrowd-s3-lawlinks-dev'
    return all_data_table, src_bucket, dstn_bucket, output_bucket


def getDocumentFromDynamoDb (all_data_table,unique_id,stage):
    print("try to get ", unique_id , " from Table: ",all_data_table)
    responseBodyDB = dynamodb_client.query(
        TableName=all_data_table,
        KeyConditionExpression="category = :category AND unique_id = :unique_id",
        ExpressionAttributeValues={
            ":unique_id": {
                "N": str(unique_id)
            },
            ":category": {
                "S": "DOCUMENT"
            }
        }
    )
    print("DynamoDB Response ",stage,": ",responseBodyDB) 
        
    return responseBodyDB['Items'][0]


def extract_info_from_resources(context):
    resource = context.invoked_function_arn
    if '_dev' in resource:
        return 'dev'
    elif '_prod' in resource:
        return 'prod'
    return None

def normalize_text(text):
    """
    Normalisiert den Text, indem übliche Störungen entfernt werden:
    - Entfernt Mehrfach-Leerzeichen
    - Entfernt Zeilenumbrüche
    - Wandelt in Kleinschreibung um
    """
    normalized = re.sub(r'\s+', '  ', text).replace('\n', '  ').lower().strip()
    return normalized

def get_document_id(event):
    if 'Records' in event and all('body' in record for record in event['Records']):
        try:
            # Extract the message body from the first record
            sqs_message_body_str = event['Records'][0]['body']
            sqs_message_body = json.loads(sqs_message_body_str)
            if isinstance(sqs_message_body, str):
                sqs_message_body = json.loads(sqs_message_body)
            elif isinstance(sqs_message_body, dict):
                sqs_message_body = sqs_message_body
            else:
                print(f"Error decoding JSON: {e}")
                return None
            if "unique_id" in sqs_message_body:
                unique_id = sqs_message_body["unique_id"]
            return unique_id
        except json.JSONDecodeError as e:
            # Handle invalid JSON
            print(f"Error decoding JSON: {e}")
            return None

def extract_links_from_pdf(pdf_path):
    links = []
    try:
        # Open the PDF file
        pdf_document = fitz.open(pdf_path)
 
        text_previous_pages = ""
        # Iterate through each page of the PDF
        for page_num in range(len(pdf_document)):
            page = pdf_document.load_page(page_num)
            
            # Get annotations from the page
            page_links = page.get_links()
            page_coords = page.bound()
            text = page.get_text()

            for link in page_links:
                link_dict = {}
                
                # Check if 'uri' key exists in the link dictionary
                if 'uri' in link:
                    link_dict['link'] = link['uri']
                else:
                    # If 'uri' key is missing, skip this link
                    continue
                #link_dict['link'] = link['uri']
                rect = link['from']
                h = rect.height * 0.1
                smaller = rect + (0, h, 0, -h)  
                text = page.get_textbox(smaller)
                text = " ".join(text.split())
                link_dict['text'] = text

                y_before = rect.y0
                rect_above = deepcopy(page_coords)
                rect_above.y1 = y_before
                text_above = page.get_text(clip=rect_above)

                rect_before = deepcopy(page_coords)
                rect_before.y1 = rect.y1
                rect_before.y0 = rect.y0
                rect_before.x1 = rect.x0
                text_before = page.get_text(clip=rect_before)

                complete_text_before = text_previous_pages + text_above + text_before
                previous_ocurrences = complete_text_before.count(text)
                link_dict['ocurrence_num'] = previous_ocurrences

                links.append(link_dict)
            text_previous_pages += page.get_text()
        # Close the PDF document
        pdf_document.close()
    except Exception as e:
        raise e
        #print("Error extracting links from PDF:", str(e))
    
    return links


def find_link_indices(anonymized_text, extracted_links):
    """
    Find indices of links in the anonymized text.
    
    Args:
    - anonymized_text (str): The anonymized text content.
    - extracted_links (list): List of dictionaries containing 'link' and 'text'.
    
    Returns:
    - list: List of dictionaries with 'link', 'text', and 'indices'.
    """

    links_with_indices = []
    for link_info in extracted_links:
        link = link_info['link']
        text = link_info['text']
        position = link_info['ocurrence_num']

        # Construct a regular expression pattern to match the text, accounting for dashes and variable spaces
        pattern = re.escape(text).replace(r'\ ', r'\s*').replace(r'\-', r'[-\s]')

        # Find all occurrences of the pattern in the anonymized text
        matches = list(re.finditer(pattern, anonymized_text))
        print(f"Link: {link}, Text: \"{text}\", Expected position: {position}, Matches found: {len(matches)}")

        if matches:
            if position < len(matches):
                match = matches[position]
            else:
                print(f"Warning: Position {position} is out of bounds for matches. Using the last match instead.")
                match = matches[-1]

            start_index = match.start()
            end_index = match.end()

            # Update link_info with start_index and end_index
            link_info['start_index'] = start_index
            link_info['end_index'] = end_index

        links_with_indices.append(link_info)

    return links_with_indices



def lambda_handler(event, context):
    section_keys = ['header', 'tenor', 'tatbestand', 'gruende', 'entschgruende','entscheidungsgruende', 'rechtsbelehrung', 'rechtsmittel']

    decision_id = get_document_id(event)

    stage = extract_info_from_resources(context)
    all_data_table, src_bucket, s3_bucket_json_objects, output_bucket = getStageVariables(stage)
    
    """ try:
        # Get the object from S3
        json_key = str(decision_id) + ".json"
        lawlink_json_key = "lawlinks/"+ json_key
        lawlink_json_key = "lawlinks/"+ json_key
        response_lawlink = s3_client.get_object(Bucket=s3_bucket_json_objects, Key=lawlink_json_key)
    except Exception as e:
        print(e)
        response_lawlink = None

    if not response_lawlink:
        print("Lawlinks could not be loaded...")
    
    # load arguments from DynamoDB?"""

    responseBody = dynamodb_client.query(
        TableName=all_data_table,
        KeyConditionExpression="category = :category AND unique_id = :base_unique_id",
        ExpressionAttributeValues={
            ":base_unique_id": {
                "N": str(decision_id)
            },
            ":category": {
                "S": "DOCUMENT"
            },
        },
    )
    # response body contains the vorinstanzen_reference

    # check if it is True otherwise this action cannot be completed
    thisDocument = None
    if 'Items' in responseBody and len(responseBody['Items']) > 0:
        thisDocument = responseBody['Items'][0]

    if not thisDocument:
        return {
            'statusCode': 400,
            'body': 'DynamoDB did not find given document id'
        }

    if not 'vorinstanzen_reference' in thisDocument:
        return {
            'statusCode': 400,
            'body': 'vorinstanzen_reference does not exist in this document'
        }

    vorinstanzen_ref = thisDocument['vorinstanzen_reference']
    if 'S' in vorinstanzen_ref:
        vorinstanzen_ref = vorinstanzen_ref['S']
    print(vorinstanzen_ref)
    # creating pdf file that includes the text
    
    
    doc = fitz.open()
    page = doc.new_page()
    where = fitz.Point(10, 10)
    page.insert_text(where, vorinstanzen_ref, fontsize=10)
    f0 = open('/tmp/input.pdf', 'x')
    f0.close()
    doc.save('/tmp/input.pdf')

    api_url = 'https://api.lawlink.de/api/v1/iurcrowdpdfprocessor'
    converted_pdf = convert_pdf(api_url, '/tmp/input.pdf')
    if not converted_pdf:
        return {
            'statusCode': 400,
            'body': 'PDF conversion failed'
        }
    with open('/tmp/input.pdf', "wb") as local_pdf:
            local_pdf.write(converted_pdf)
    extracted_links = extract_links_from_pdf('/tmp/input.pdf')
    links_with_indices = find_link_indices(vorinstanzen_ref, extracted_links)

    # add links with indices to lawlink json file
    # get lawlink file from /lawlink

    json_key = str(decision_id) + ".json"
    lawlink_json_key = "lawlinks/"+ json_key
    try:
        # Get the object from S3
        response_lawlink = s3_client.get_object(Bucket=s3_bucket_json_objects, Key=lawlink_json_key)
    except Exception as e:
        print(e)
        response_lawlink = None
    lawlinks = None
    
    if response_lawlink:
        lawlinks_data = response_lawlink['Body'].read().decode('utf-8')
        lawlinks = json.loads(lawlinks_data)
    if lawlinks is not None:
        lawlinks['lawlinks']['vorinstanzen'] = links_with_indices
    else:
        lawlinks = {
            'lawlinks': {
                'vorinstanzen': links_with_indices
            }
        }
    # save data to S3
    s3_client.put_object(Bucket=s3_bucket_json_objects, Key=lawlink_json_key, Body=json.dumps(lawlinks))
    
    # also put in s3 bucket for lawlinks
    try:
        # Get the object from S3
        response_lawlink = s3_client.get_object(Bucket=output_bucket, Key=lawlink_json_key)
    except Exception as e:
        print(e)
        response_lawlink = None
    if response_lawlink:
        lawlinks_data = response_lawlink['Body'].read().decode('utf-8')
        lawlinks = json.loads(lawlinks_data)
    else:
        lawlinks = {
            'lawlinks': {
                'vorinstanzen': links_with_indices
            }
        }
    s3_client.put_object(Bucket=output_bucket, Key=lawlink_json_key, Body=json.dumps(lawlinks))
    
    # Return a successful response
    return {
        'statusCode': 200,
        'body': "Done"
    }
