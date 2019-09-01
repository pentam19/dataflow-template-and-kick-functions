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
    PROJECT = '[project-id]'
    JOB_NAME='dataflowtest'

    jobs= service.projects().jobs()

    # Check Running JOB
    req = jobs.list(projectId=PROJECT)
    res_ranjob = req.execute()
    
    for j in res_ranjob['jobs']:
        if j['name'] == JOB_NAME and j['currentState'] == 'JOB_STATE_RUNNING':
            jobid = j['id']
            # DRAIN JOB
            body = {
                "requestedState": "JOB_STATE_DRAINED"
            }
            req = jobs.update(projectId=PROJECT, jobId=jobid, body=body)
            response = req.execute()
            print(response)
            
    return 'Finish!!!'