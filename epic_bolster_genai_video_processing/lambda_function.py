# main function



import boto3
import time
import json
import urllib.request
from botocore.client import Config

session = boto3.session.Session(region_name='us-west-2')
s3 = session.client('s3', config=boto3.session.Config(signature_version='s3v4'))
lambda_client = boto3.client('lambda')
sm_runtime = boto3.client('sagemaker-runtime')

transcribe_client = boto3.client('transcribe')

FMC_URL = "https://bedrock-runtime.us-west-2.amazonaws.com"
bedrock = boto3.client("bedrock-runtime","us-west-2")

def extract_text_from_img(bucket,key):
  data=""
  try:
    s3_b = boto3.resource("s3")
    temp_path=key[0]
    temp_paths=temp_path.split("/")[-1]
    local_filepath = '/tmp/' + str(temp_paths)
    s3_b.Bucket(bucket).download_file(temp_path, local_filepath)
    img_data = open(local_filepath, 'rb').read()
    textract = session.client("textract")
    response = textract.detect_document_text(Document={"Bytes": img_data})
    extracted_text = ""
    for block in response["Blocks"]:
      if block["BlockType"] == "LINE":
        extracted_text += block["Text"] + "\n"
    
    print(f"extracted_text {extracted_text}")
    pre_prompt= extracted_text + "The Given Text is extract from Image this is reated to some error please summarize and reframe in simple sentences the given text."
    print("*** Pre prompt ***",pre_prompt)
    data = generate_text(pre_prompt)
    
    print("Reframe the textract question {data}")
  except Exception as e:
    print(f"extract_text_from_img Error {e}")
  
  return data

def create_presigned_url(bucket,key):
  try:
    s3_details={'Bucket': bucket, 'Key': key}
    s3_details=json.dumps(s3_details)
    response = lambda_client.invoke(
    FunctionName='presinged_image',
    InvocationType='RequestResponse',
    Payload=s3_details)
    s3_url=response['Payload'].read().decode('utf-8')
    print("s3_url",s3_url)

    # s3_url_Json = json.loads(s3_url)
    presigned_urls = s3_url    #s3_url_Json["presigned_url"]
    presigned_urls=presigned_urls.strip("\"")
    print("presigned_url",presigned_urls)
    
    return presigned_urls
  except Exception as e:
    print("create_presigned_url -",create_presigned_url)
    raise Exception(e)

def image_caption_sagemaker_model(bucket,key):
  presigned_url=create_presigned_url(bucket,key)
  print("********presigned_url***********",presigned_url)
  prompt_template="User:{prompt}![]({image})<end_of_utterance>\nAssistant:"
  parameters = {
    "do_sample": True,
    "top_p": 0.2,
    "temperature": 0.4,
    "top_k": 50,
    "max_new_tokens": 512,
    "stop": ["User:","<end_of_utterance>"]
  }
  prompt='can you describe image'
  parsed_prompt = prompt_template.format(image=presigned_url,prompt=prompt)
  paylaod={"inputs":parsed_prompt,"parameters":parameters}
  json_payload=json.dumps(paylaod)
  
  response = sm_runtime.invoke_endpoint(EndpointName='huggingface-pytorch-tgi-inference-2024-03-01-05-40-31-943', ContentType='application/json', Body=json_payload)
  result=json.loads(response['Body'].read().decode('utf-8'))
  final_result=result[0]["generated_text"][len(parsed_prompt):].strip()
  print("*********result************",result)
  
  print("*********final_result************",final_result)
  
  return final_result
  
def generate_text(prompt):
  enclosed_prompt = "Human: " + prompt + "\n\nAssistant:"
  body = {
      "prompt": enclosed_prompt,
      "max_tokens_to_sample": 200,
      "temperature": 0.5,
      "stop_sequences": ["\n\nHuman:"],
  }

  response = bedrock.invoke_model(
      modelId="anthropic.claude-v2", body=json.dumps(body)
  )

  response_body = json.loads(response["body"].read())
  completion = response_body["completion"]

  return completion

job_name = 'epic_bolster_gen_ai_test1'
job_uri = 's3://epic-bolster-images/video/epic1.mp4'

language_code = 'en-US'

def delete_transcription_job(job_name):
  
  response = transcribe_client.get_transcription_job(TranscriptionJobName=job_name)
  print('fghjk')
  if response:
    transcribe_client.delete_transcription_job(TranscriptionJobName=job_name)

def transcribe_video(job_name, job_uri):
  
  transcribe_client.start_transcription_job(TranscriptionJobName=job_name, Media={'MediaFileUri': job_uri}, MediaFormat='mp4', LanguageCode='en-US')

  while True:
    status = transcribe_client.get_transcription_job(TranscriptionJobName=job_name)
    if status['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
      break

  if status['TranscriptionJob']['TranscriptionJobStatus'] == 'COMPLETED':
    response = transcribe_client.get_transcription_job(TranscriptionJobName=job_name)
    json_url = response['TranscriptionJob']['Transcript']['TranscriptFileUri']
    response = urllib.request.urlopen(json_url)
    data = json.loads(response.read())
    text = data['results'] #['transcripts'][0]['transcript']
    print("data",text)
    return text

def image_desc(event):
  try:
    img_desc=[]
    query=''
    query=event['query']
    if "files" in event.keys():
      bucket=event["files"]["bucket"]
      key = event["files"]["key"]
      for keyname in key:
        img_valid_extensions = ['.jpg', '.jpeg', '.png']
        if keyname.endswith(tuple(img_valid_extensions)):
          print("start image processing")
          img_desc1=image_caption_sagemaker_model(bucket,keyname)
          img_desc.append(img_desc1)
          print("*************img_desc*********",img_desc)
        else:
          return "File Format error"
          
    pre_prompt=extract_text_from_img(bucket,key)
  
    prompt= ' '.join(img_desc)
    prompts="Here is Image descriptation "+str(prompt) + str(pre_prompt) + query + " understand the question and give me details solution to bedug issue"
    print('Image Prompt ',prompts)
    print("prompt generate done.....")
    return prompts
  except Exception as e:
    raise Exception(e)
def lambda_handler(event, context):
  print("*******event********",event)
  try:
    query=event['query']
    
    if "files" in event.keys():
      key = event["files"]["key"]
      video_valid_extensions =['.mp4','.avi','.mov']
      if key[0].endswith(tuple(video_valid_extensions)):
        s3_video_details={'Bucket': event["files"]["bucket"], 'Key': event["files"]["key"][0],'Query':query}
        s3_video_details=json.dumps(s3_video_details)
        video_response = lambda_client.invoke(
        FunctionName='epic_gen_ai_video_without_audio',
        InvocationType='RequestResponse',
        Payload=s3_video_details)
        video_prompt=video_response['Payload'].read().decode('utf-8')
        print("video_prompt",video_response)
        
        # job_name = 'epic_bolster_gen_ai_test1'
        # job_uri = 's3://epic-bolster-images/video/epic1.mp4'
        # delete_transcription_job(job_name)
        # transcript = transcribe_video(job_name, job_uri)
        # prompt= transcript + 'sumarize the given text .'
        print('************* video_prompt **************',video_prompt)
        
        data = generate_text(video_prompt)
        return data
      else:
        prompt=image_desc(event)
        print('************* PROMPT **************',prompt)
        data = generate_text(prompt)
        print(f"Bedrock img answer {data}")
        return data
  
    else:
      print("call bedrock for user query processing")
      data = generate_text(query)
      print("User Query Resp.....",data)
      return data
  except Exception as e:
    print('ERROR ---',e)
    raise Exception(e)
      

    
  
  # img_desc=[]
  # query=''
  # query=event['query']
  # if "files" in event.keys():
  #   try:
  #     bucket=event["files"]["bucket"]
  #     key = event["files"]["key"]
  
  #     for keyname in key:
  #       img_valid_extensions = ['.jpg', '.jpeg', '.png']
  #       video_valid_extensions =['.mp4','.avi','.mov']
        
  #       if keyname.endswith(tuple(img_valid_extensions)):
  #         print("start image processing")
  #         img_desc1=image_caption_sagemaker_model(bucket,keyname)
  #         img_desc.append(img_desc1)
  #         print("*************img_desc*********",img_desc)
          
  #       else:
  #         return "File Format error"
  #       if key.endswith(tuple(video_valid_extensions)):
  #         print("video image processing")
  #         job_name = 'epic_bolster_gen_ai_test1'
  #         job_uri = 's3://epic-bolster-images/video/epic1.mp4'
  #         delete_transcription_job(job_name)
  #         transcript = transcribe_video(job_name, job_uri)
  #         prompt= transcript + 'sumarize the given text .'
  #         img_desc.append(prompt)
  #         # data = generate_text(query)
  #         # print('data',data)
  #       else:
  #         return "File Format error"
          
  #     prompt= ' '.join(img_desc) + query
  #     prompt=str(prompt)+query
  #     data = generate_text(prompt)
  #     print('data',data)
  #     return data
  #   except Exception as e:
  #     return(e)
  # else:
  #   print("call bedrock for user query processing")
  #   data = generate_text(query)
  #   return data
