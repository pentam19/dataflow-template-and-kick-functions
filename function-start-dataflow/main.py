from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials


def hello_world(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    credentials = GoogleCredentials.get_application_default()
    service = build('dataflow', 'v1b3', credentials=credentials)

    # Set the following variables to your values.
    JOBNAME = 'dataflowtest'
    PROJECT = '[project-id]'
    BUCKET = '[bucket]'
    TEMPLATE = 'mytemplate'

    GCSPATH="gs://{bucket}/templates/{template}".format(bucket=BUCKET, template=TEMPLATE)
    BODY = {
        "jobName": "{jobname}".format(jobname=JOBNAME),
        #"parameters": {
        #    "inputFile" : "gs://{bucket}/input/my_input.txt",
        #    "outputFile": "gs://{bucket}/output/my_output".format(bucket=BUCKET)
        # },
        "environment": {
            "tempLocation": "gs://{bucket}/temp".format(bucket=BUCKET),
            #"zone": "us-central1-f"
        }
    }

    request = service.projects().templates().launch(projectId=PROJECT, gcsPath=GCSPATH, body=BODY)
    response = request.execute()

    print(response)