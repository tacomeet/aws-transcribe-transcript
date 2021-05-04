import json 

def convert_transcript(infile, outfile):
    speaker_start_times = {}
    lines = []
    line = ''
    time = 0
    speaker = 'spk_1'
    recent_speaker = 'spk_1'

    import datetime
    print(f"Filename: {infile}")

    with open(outfile, "w+") as writeFile:
        with open(infile) as readFile:
            data = json.loads(readFile.read())
            results = data.get("results")

            try:
                speaker_labels = results["speaker_labels"]
            except KeyError:  # speaker labels are off; just return the transcript
                transcript = results.get("transcripts")[0].get("transcript")
                writeFile.write(f"{transcript}")
                return

            for label in speaker_labels.get("segments", []):
                for item in label.get("items", []):
                    speaker_start_times.update({item.get("start_time"): item.get("speaker_label", "Anon")})

            items = results.get("items", [])
            for idx, item in enumerate(items):
                
                if item.get("start_time"):  # This is a spoken item
                    speaker = speaker_start_times.get(item.get("start_time"), "Anon")
                    if speaker == recent_speaker:
                        line+=f" {item.get('alternatives')[0].get('content')}"  # Append the content to line and repeat
                    else:  # New speaker
                        lines.append({'speaker': recent_speaker, 'line': line, 'time': time})
                        print(f"[{time}] {recent_speaker}: {line}")
                        recent_speaker = speaker
                        line=item.get('alternatives')[0].get('content')
                        time=item.get('start_time')
                elif item.get("type") == "punctuation":
                    line+=item.get('alternatives')[0].get('content')

            lines.append({'speaker': speaker, 'line': line, 'time': time})
            sorted_lines = sorted(lines, key=lambda k: float(k['time']))
            for line_data in sorted_lines:
                line = '[' + str(
                    datetime.timedelta(seconds=int(round(float(line_data['time']))))) + '] ' + line_data.get(
                    'speaker') + ': ' + line_data.get('line')
                writeFile.write(f"{line}\n\n")

def lambda_handler(event, context):
    import os, boto3
    
    s3_client = boto3.client('s3')

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        localkey = os.path.basename(key)
        txtfile = f"output/{localkey}.txt"
        download_path = f"/tmp/{localkey}"
        upload_path = f"/tmp/{localkey}.txt"
        s3_client.download_file(bucket, key, download_path)
        convert_transcript(download_path, upload_path)
        s3_client.upload_file(upload_path, f'{bucket}', txtfile)
        
    return {'statusCode': 200, 'body': json.dumps('Transcription run.')}

