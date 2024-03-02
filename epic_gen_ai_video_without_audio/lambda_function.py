import json

import boto3
import os
import hashlib
import subprocess
s3 = boto3.client('s3')
lambda_client=boto3.client('lambda')
sm_runtime = boto3.client('sagemaker-runtime')


def all_frame_list(bucket_name,folder_name):
    # get all frame from s3 
    try:
        response = s3.list_objects(Bucket=bucket_name, Prefix=folder_name)
        object_keys = [obj['Key'] for obj in response['Contents']]
        print('all_frame_list s3 frame',object_keys)
        # object_keys=object_keys[1:]
        return object_keys
    except Exception as e:
        print('Failed to ge all frame list all_frame_list():',e)
        raise Exception('Failed to get video Frame list , please check all_frame_list() function')

def get_presigned_url(bucket_name,folder_name):
    # create presigned url of all frames
    presigned_url_list=[]
    object_keys=all_frame_list(bucket_name,folder_name)
    try:
        for frame_keys in object_keys:
            frames_bucket_keys={}
            print('Frame Keys',frame_keys)
            frames_bucket_keys={'Bucket': bucket_name, 'Key': frame_keys}
            frames_bucket_keys=json.dumps(frames_bucket_keys)
            response = lambda_client.invoke(
            FunctionName='presinged_image',
            InvocationType='RequestResponse',
            Payload=frames_bucket_keys)
            s3_url=response['Payload'].read().decode('utf-8')
            print("s3_url",s3_url)
            presigned_url_list.append(s3_url)
        print('Presigned URL List =',presigned_url_list)
        return presigned_url_list
    except Exception as e:
        print('Failed to create presigned url for each frame get_presigned_url(): ',e)
        raise Exception('Failed to create Pre-signed Url , please check  get_presigned_url() function')

def image_caption_sagemaker_model(frame_presigned_url):
    # invoke sagemaker model endpoint for image caption
    try:
        prompt_template="User:{prompt}![]({image})<end_of_utterance>\nAssistant:"
        parameters = {
            "do_sample": True,
            "top_p": 0.2,
            "temperature": 0.4,
            "top_k": 50,
            "max_new_tokens": 512,
            "stop": ["User:","<end_of_utterance>"]
        }
        prompt='What is in this image?'
        parsed_prompt = prompt_template.format(image=frame_presigned_url,prompt=prompt)
        paylaod={"inputs":parsed_prompt,"parameters":parameters}
        json_payload=json.dumps(paylaod)
      
        response = sm_runtime.invoke_endpoint(EndpointName='huggingface-pytorch-tgi-inference-2024-03-02-08-24-20-625', ContentType='application/json', Body=json_payload)
        result=json.loads(response['Body'].read().decode('utf-8'))
        final_result=result[0]["generated_text"][len(parsed_prompt):].strip()
    
        print("*********final_result************",final_result)
        return final_result
    except Exception as e:
        print('Image Caption model Failed ',e)
        raise Exception('Image Caption model Failed , please check image_caption_sagemaker_model() function')
        
def image_desc(bucket_name,folder_name,user_query):
    # pass input saagemaker model and get response from model
    try:
        img_desc=[]
        presigned_url_list=get_presigned_url(bucket_name,folder_name)
        for presigned_urls in presigned_url_list:
            frame_presigned_url=presigned_urls.strip("\"")
            print("start image processing",frame_presigned_url)
            img_desc1=image_caption_sagemaker_model(frame_presigned_url)
            img_desc.append(img_desc1)
            print("*************All img_desc *********",img_desc)

        prompt= ' '.join(img_desc)
        prompts="Here is Image descriptation "+str(prompt) + user_query + " understand the question and give me details solution to bedug issue"
        print('Image Prompt ',prompts)
        print("prompt generate done.....")
        return prompts
    except Exception as e:
        raise Exception(e)

def remove_duplicate_frames(frames_dir):
    # remove duplicate frame from video
    # Initialize a set to store the hashes of the frames
    frame_hashes = set()

    # Iterate over the frames in the frames directory
    for frame_file in os.listdir(frames_dir):
        frame_path = os.path.join(frames_dir, frame_file)

        # Calculate the MD5 hash of the frame
        with open(frame_path, 'rb') as f:
            frame_hash = hashlib.md5(f.read()).hexdigest()

        # Check if the hash is already in the set
        if frame_hash in frame_hashes:
            # Remove the duplicate frame
            os.remove(frame_path)
        else:
            # Add the hash to the set
            frame_hashes.add(frame_hash)
            
def video_to_frame(bucket_name,video_key):
    try:
        ffmpeg_path = '/opt/ffmpeglibs/ffmpeg'
        temp_dir = '/tmp/video.mp4'
        s3_output_path=video_key.rsplit('/',1)
        s3_output_path=str(s3_output_path[0])+'/'+'frames'
        print('s3_output_path',s3_output_path)
        s3.download_file(bucket_name, video_key, temp_dir)
        # Create the output directory if it doesn't exist
        output_dir = '/tmp/frames'
        os.makedirs(output_dir, exist_ok=True)
        
        # Execute the ffmpeg binary with the appropriate arguments to convert the video into frames
        process = subprocess.Popen([ffmpeg_path, '-i', temp_dir, '-vf', 'fps=2', os.path.join(output_dir, 'frame-%d.png')], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        # process=subprocess.run([ffmpeg_path,'-i', temp_dir,'-vf', 'fps=1', os.path.join(output_dir, 'frame_%04d.png')  ])
        print('frame converion done ....',process.returncode)
        print(os.listdir(output_dir))
        
        if process.returncode == 0:
            # Remove duplicate 
            remove_duplicate_frames(output_dir)
            # Upload the unique frames to S3
            for frame_file in os.listdir(output_dir):
                frame_path = os.path.join(output_dir, frame_file)
                s3.upload_file(frame_path, bucket_name, f'{s3_output_path}/{frame_file}')
                print('Frame uploaded into s3 ....')
                
                return s3_output_path
        else:
            print('Frame uploaded into s3 ....',stderr.decode('utf-8'))
            # Return the error message
            return {
                'statusCode': 500,
                'body': stderr.decode('utf-8')
            }
                
    except Exception as e:
        print('Failed to convert video into frame ',e)
        raise Exception('Failed to convert video to frame please check video_to_frame() function')
        
def lambda_handler(event, context):
    print('lambd aevent',event)
    
    bucket_name = event['Bucket']
    video_key = event['Key']
    user_query = event['Query']
    
    folder_name=video_to_frame(bucket_name,video_key)
    
    video_prompts=image_desc(bucket_name,folder_name,user_query)
    print('video_prompts',video_prompts)
    return video_prompts

